use std::collections::{HashMap, HashSet};
use std::iter::Iterator;

mod command;
mod option;
mod param;
mod protocol_extensions;
use anyhow::Context;
use anyhow::Result;

#[cfg(test)]
mod test;

pub use command::Command;
pub use command::CommandParams;
pub use command::MixedSubcommand;
pub use command::OperationRatio;
#[cfg(feature = "user-profile")]
pub use command::{OpWeight, PREDEFINED_INSERT_OPERATION};
pub use option::ErrorsOption;
pub use option::LogOption;
pub use option::ThreadsInfo;
use regex::Regex;
use scylla::client::session::Session;
use scylla::cluster::metadata::ConsistencyMode;
use scylla::cluster::ClusterState;
use scylla::statement::Consistency;

use crate::settings::command::print_help;

use self::command::parse_command;
use self::option::ColumnOption;
use self::option::ModeOption;
use self::option::NodeOption;
use self::option::PopulationOption;
use self::option::RateOption;
use self::option::SchemaOption;
use self::option::TransportOption;
use self::protocol_extensions::fetch_protocol_features;

pub struct CassandraStressSettings {
    pub command: Command,
    pub command_params: CommandParams,
    pub node: NodeOption,
    pub rate: RateOption,
    pub mode: ModeOption,
    pub schema: SchemaOption,
    pub column: ColumnOption,
    pub population: PopulationOption,
    pub log: LogOption,
    pub transport: TransportOption,
    pub errors: ErrorsOption,
}

impl CassandraStressSettings {
    pub fn print_settings(&self) {
        println!("******************** Stress Settings ********************");
        self.command_params.print_settings(&self.command);
        self.rate.print_settings();
        self.mode.print_settings();
        self.node.print_settings();
        self.schema.print_settings();
        self.column.print_settings();
        self.population.print_settings();
        self.log.print_settings();
        self.transport.print_settings();
        self.errors.print_settings();
        println!();
    }

    pub async fn create_schema(&self, session: &Session) -> Result<()> {
        #[cfg(feature = "user-profile")]
        if let Some(user) = &self.command_params.user {
            return user.create_schema(session).await;
        }

        if matches!(self.command, Command::Write | Command::CounterWrite) {
            session
                .query_unpaged(self.schema.construct_keyspace_creation_query(), ())
                .await?;
        }

        session.use_keyspace(&self.schema.keyspace, true).await?;

        match self.command {
            Command::Write => {
                session
                    .query_unpaged(
                        self.schema
                            .construct_table_creation_query(&self.column.columns),
                        (),
                    )
                    .await
                    .context("Failed to create standard table")?;
            }
            Command::CounterWrite => {
                session
                    .query_unpaged(
                        self.schema
                            .construct_counter_table_creation_query(&self.column.columns),
                        (),
                    )
                    .await
                    .context("Failed to create counter table")?;
            }
            _ => (),
        }

        Ok(())
    }

    /// Reports the keyspace's consistency mode, so the mode a run actually measured is
    /// recorded alongside its numbers, and refuses to start a run that would not measure
    /// what it claims to.
    ///
    /// The mode comes from the driver rather than from `system_schema` on purpose. The
    /// driver reports [`ConsistencyMode::Global`] only when it *both* negotiated the
    /// `TABLETS_ROUTING_V2` protocol extension with this cluster *and* read
    /// `consistency = 'global'` for the keyspace - it does not even select that column
    /// otherwise. One value therefore proves both halves of leader-aware routing, including
    /// the half no server-side query can see: that this build of the driver supports it at
    /// all. Asking the server directly would not: ScyllaDB 2026.2.x records
    /// `consistency = 'global'` in `system_schema.scylla_keyspaces` while advertising only
    /// `TABLETS_ROUTING_V1`, and a driver without leader-aware routing reads back exactly
    /// the same rows as one with it.
    ///
    /// When `consistency=global` was requested, anything short of that is a hard startup
    /// failure. Every way this can go wrong otherwise produces a full, plausible,
    /// meaningless result set:
    /// - `CREATE KEYSPACE IF NOT EXISTS` no-ops over a leftover eventually consistent
    ///   keyspace from an earlier run;
    /// - a `read`-only run never creates the keyspace at all;
    /// - the server lacks `--experimental-features=strongly-consistent-tables`;
    /// - the cluster feature gating strongly consistent tables is not on yet, which is
    ///   the case until every node carries that flag;
    /// - the server takes `consistency = 'global'` but advertises no
    ///   `TABLETS_ROUTING_V2_EXPERIMENTAL`, so the driver never sees a leader-ordered
    ///   replica list and spreads the load over followers.
    ///
    /// When `consistency` was not requested the mode is only reported - existing eventually
    /// consistent runs must keep working unchanged.
    pub async fn verify_consistency_mode(&self, session: &Session) -> Result<()> {
        // The DDL above may have raced the background metadata refresh, so force one before
        // reading the mode back: this is the same snapshot the driver's own routing
        // decisions are made from.
        session
            .refresh_metadata()
            .await
            .context("Failed to refresh cluster metadata")?;

        let keyspace = &self.schema.keyspace;
        let cluster_state = session.get_cluster_state();
        // `None` means the keyspace does not exist at all, which is a different thing from
        // existing as eventually consistent, and the two get different messages below.
        let mode = cluster_state
            .get_keyspace(keyspace)
            .map(|ks| ks.consistency_mode.clone());

        // `None` and `Eventual` fail for different reasons and deserve different words.
        let reported_mode = match &mode {
            Some(mode) => format!("{mode:?}"),
            None => String::from("unknown (keyspace not found in cluster metadata)"),
        };
        println!("Keyspace '{keyspace}' consistency mode: {reported_mode}");

        // `ConsistencyMode` is `#[non_exhaustive]`: match the one variant that means strong
        // consistency rather than enumerating the others, so a future variant is treated as
        // "not strongly consistent" instead of failing to compile.
        let strongly_consistent = matches!(mode, Some(ConsistencyMode::Global));

        if !strongly_consistent {
            if self.schema.wants_strong_consistency() {
                // The mode alone cannot say which of the causes applied, so ask the node
                // whether it could ever route to a leader before giving up.
                let diagnosis = self.diagnose_missing_strong_consistency().await;
                anyhow::bail!(
                    "Requested consistency=global, but the driver does not see keyspace \
                     '{keyspace}' as strongly consistent (mode: {reported_mode}). This run \
                     would not measure strong consistency. The driver reports Global only \
                     once it has both negotiated TABLETS_ROUTING_V2 with this cluster and \
                     read consistency='global' for the keyspace, so any of these breaks \
                     it:\n\
                     - the server does not run with \
                     --experimental-features=strongly-consistent-tables;\n\
                     - the cluster feature gating strongly consistent tables is not enabled \
                     yet - it turns on only once every node carries that flag, so a \
                     partially upgraded cluster lands here;\n\
                     - the server does not advertise TABLETS_ROUTING_V2_EXPERIMENTAL, which \
                     is a capability separate from accepting consistency='global' (ScyllaDB \
                     2026.2.x has the second without the first);\n\
                     - keyspace '{keyspace}' already exists as an eventually consistent \
                     keyspace (CREATE KEYSPACE IF NOT EXISTS will not upgrade it - drop it \
                     first);\n\
                     - the keyspace is not tablet-based (non-tablet keyspaces reject the \
                     consistency option; SimpleStrategy may not get tablets).\n\
                     DDL used: {ddl}{diagnosis}",
                    ddl = self.schema.construct_keyspace_creation_query(),
                );
            }

            return Ok(());
        }

        println!(
            "Leader-aware routing: enabled (the driver negotiated TABLETS_ROUTING_V2 with \
             this cluster and keyspace '{keyspace}' is strongly consistent)"
        );

        // Keyed on the mode the keyspace actually has, not on what was requested: a
        // pre-provisioned strongly consistent keyspace behaves the same whether or not
        // `consistency=global` was passed, since `CREATE KEYSPACE IF NOT EXISTS` no-ops over
        // it and the mode is a property of the keyspace, not of the CLI flag.
        //
        // The consistency level first: when it is wrong the run cannot start at all, and
        // routing advice for a run that will not happen is just noise.
        self.verify_consistency_level()?;
        self.warn_on_datacenter_preference(&cluster_state);

        Ok(())
    }

    /// Warns when a preferred datacenter quietly narrows leader-aware routing to a fraction
    /// of the tablets.
    ///
    /// A tablet's Raft leader can be in any datacenter - a globally consistent keyspace gains
    /// nothing from locality, so nothing pins the leader near the client. Leader-aware routing
    /// therefore ranks the leader above distance, but only among hosts the load balancing
    /// policy would contact at all. `cql-stress` never enables datacenter failover, so with
    /// `-node datacenter=` a leader in any other datacenter is vetoed: the request goes to a
    /// local replica instead and the server forwards it to the leader. That forward is the
    /// extra hop the whole benchmark exists to avoid, and it is taken for every tablet whose
    /// leader sits elsewhere - so a multi-datacenter run measures a blend of leader-routed and
    /// forwarded requests whose ratio drifts as ScyllaDB rebalances leaders.
    ///
    /// None of that is visible in the numbers: the mode still reads `Global`, leader-aware
    /// routing genuinely is enabled, and the coordinator distribution is still skewed - just
    /// toward local replicas rather than leaders.
    ///
    /// Restricting to one datacenter costs nothing when the cluster only has one, which is the
    /// common case and must stay silent. A preferred *rack* is not affected: the leader
    /// outranks rack, and only the datacenter can veto it.
    fn warn_on_datacenter_preference(&self, cluster_state: &ClusterState) {
        let Some(preferred_dc) = self.node.datacenter.as_deref() else {
            return;
        };

        let mut datacenters: Vec<&str> = cluster_state
            .get_nodes_info()
            .iter()
            .filter_map(|node| node.datacenter.as_deref())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if datacenters.len() < 2 {
            return;
        }
        datacenters.sort_unstable();

        let keyspace = &self.schema.keyspace;
        println!();
        println!(
            "WARNING: keyspace '{keyspace}' is strongly consistent and this run prefers \
             datacenter '{preferred_dc}', but the cluster spans {count} datacenters \
             ({list}). A tablet's \
             Raft leader can be in any of them, and cql-stress does not enable datacenter \
             failover, so the driver will never send a request to a leader outside \
             '{preferred_dc}' - those requests go to a local replica and are forwarded to the \
             leader, which is exactly the hop leader-aware routing is meant to remove. Only \
             tablets whose leader already sits in '{preferred_dc}' are leader-routed, so this \
             run measures a mixture. Drop datacenter= to measure leader-aware routing across \
             the whole cluster. A preferred rack is fine - the leader outranks rack.",
            count = datacenters.len(),
            list = datacenters.join(", "),
        );
        println!();
    }

    /// Checks `cl=` against what a strongly consistent keyspace actually accepts.
    ///
    /// The server is far stricter here than for an eventually consistent table, and rejects
    /// per request rather than at connect time, so getting this wrong yields a run in which
    /// every single operation fails - numbers that look like a catastrophic cluster problem
    /// and are really a CLI mistake. The accepted sets are asymmetric:
    ///
    /// - **writes** take `QUORUM` and `LOCAL_QUORUM`, nothing else;
    /// - **reads** additionally take `ONE` and `LOCAL_ONE`.
    ///
    /// `ONE`/`LOCAL_ONE` are legal for reads but turn leader-aware routing off: any replica
    /// may serve such a read, so the request keeps normal spread routing. That is a warning
    /// rather than an error - the run works, it just does not measure what it set out to -
    /// and it matters because `local_one` is the default `cl`.
    fn verify_consistency_level(&self) -> Result<()> {
        let keyspace = &self.schema.keyspace;
        let cl = self.command_params.common.consistency_level;

        if matches!(cl, Consistency::Quorum | Consistency::LocalQuorum) {
            return Ok(());
        }

        anyhow::ensure!(
            matches!(cl, Consistency::One | Consistency::LocalOne),
            "Keyspace '{keyspace}' is strongly consistent, but cl={cl} is not a consistency \
             level it accepts: strongly consistent writes take QUORUM or LOCAL_QUORUM, and \
             strongly consistent reads take QUORUM, LOCAL_QUORUM, ONE or LOCAL_ONE. The \
             server would reject every operation in this run. Use cl=QUORUM."
        );

        // ONE / LOCAL_ONE from here on: accepted for reads, rejected for writes.
        let warning = match self.issues_writes() {
            Some(true) => anyhow::bail!(
                "Keyspace '{keyspace}' is strongly consistent, but cl={cl} cannot be used to \
                 write to it: the server accepts only QUORUM and LOCAL_QUORUM for strongly \
                 consistent writes and would reject every operation in this run. Use \
                 cl=QUORUM."
            ),
            Some(false) => format!(
                "WARNING: keyspace '{keyspace}' is strongly consistent, but cl={cl} disables \
                 leader-aware routing: at ONE and LOCAL_ONE any replica may serve the read, \
                 so the driver keeps normal spread routing and requests are forwarded to the \
                 leader by whichever replica received them. Note that local_one is the \
                 default cl. Use cl=QUORUM to measure strong consistency."
            ),
            // A user profile runs whatever its yaml says, so whether this run writes cannot
            // be known here. Warn about both halves instead of guessing.
            None => format!(
                "WARNING: keyspace '{keyspace}' is strongly consistent and cl={cl}. Any write \
                 in this profile will be rejected - the server accepts only QUORUM and \
                 LOCAL_QUORUM for strongly consistent writes - and the reads that do go \
                 through are not leader-routed, because at ONE and LOCAL_ONE any replica may \
                 serve them. Use cl=QUORUM to measure strong consistency."
            ),
        };

        println!();
        println!("{warning}");
        println!();

        Ok(())
    }

    /// Whether this run issues writes, which decides how strict a strongly consistent
    /// keyspace is about `cl=`. `None` for a user profile, whose operations come from a yaml
    /// file and can be either.
    fn issues_writes(&self) -> Option<bool> {
        match self.command {
            Command::Write | Command::CounterWrite | Command::Mixed => Some(true),
            Command::Read | Command::CounterRead => Some(false),
            #[cfg(feature = "user-profile")]
            Command::User => None,
            // Not workloads - they never reach this far.
            Command::Help | Command::Version | Command::VersionJson => Some(false),
        }
    }

    /// Explains, as far as it can be established from outside, why the driver does not see
    /// the keyspace as strongly consistent.
    ///
    /// A mode other than `Global` has several independent causes and the value itself cannot
    /// tell them apart. Asking the node whether it advertises `TABLETS_ROUTING_V2_EXPERIMENTAL`
    /// separates the most confusing one - a server that cannot do leader routing at all, yet
    /// happily stores `consistency = 'global'` - from an ordinary "this keyspace was not
    /// created strongly consistent".
    ///
    /// Returns a sentence to append to the failure, or an empty string when there is nothing
    /// useful to add. It runs only on the failure path, so a healthy run pays nothing for it.
    async fn diagnose_missing_strong_consistency(&self) -> String {
        let Some(node) = self.node.nodes.first() else {
            return String::new();
        };

        // The probe speaks plaintext CQL and cannot reach a TLS-only node.
        if self.transport.truststore.is_some() || self.transport.keystore.is_some() {
            return format!(
                "\nNode '{node}' was not asked whether it advertises \
                 TABLETS_ROUTING_V2_EXPERIMENTAL: the probe speaks plaintext CQL and this \
                 run uses TLS."
            );
        }

        match fetch_protocol_features(node).await {
            Ok(features) if features.tablets_v2_supported => format!(
                "\nNode '{node}' does advertise TABLETS_ROUTING_V2_EXPERIMENTAL, so this \
                 server can route to tablet leaders - the keyspace itself is what is not \
                 strongly consistent."
            ),
            Ok(_) => format!(
                "\nNode '{node}' does NOT advertise TABLETS_ROUTING_V2_EXPERIMENTAL, so it \
                 cannot hand the driver a leader-ordered replica list at all. This is a \
                 capability separate from accepting consistency='global': ScyllaDB 2026.2.x \
                 stores 'global' in system_schema.scylla_keyspaces while advertising only \
                 TABLETS_ROUTING_V1, which is exactly why the mode above reads as eventual."
            ),
            Err(error) => format!(
                "\nNode '{node}' could not be asked whether it advertises \
                 TABLETS_ROUTING_V2_EXPERIMENTAL: {error:#}"
            ),
        }
    }
}

pub enum CassandraStressParsingResult {
    // HELP, PRINT, VERSION
    SpecialCommand,
    Workload(Box<CassandraStressSettings>),
}

type ParsePayload<'a> = HashMap<String, Vec<&'a str>>;

/// Groups the commands/options and their corresponding parametes.
///
/// cassandra-stress accepts CLI args of the following pattern:
/// ./cassandra-stress COMMAND [command_param...] [OPTION [option_param...]...]
fn prepare_parse_payload(args: &[String]) -> Result<(&str, ParsePayload<'_>)> {
    let mut cl_args: ParsePayload = HashMap::new();

    let mut iter = args.iter();
    let (cmd, mut current) = {
        let cmd = iter.next().ok_or(anyhow::anyhow!("No command specified"))?;
        let current = cmd.to_lowercase();
        cl_args.insert(current.clone(), vec![]);
        (cmd, current)
    };

    for arg in iter {
        let arg: &str = arg.as_ref();

        if arg.starts_with('-') {
            anyhow::ensure!(
                !cl_args.contains_key(arg),
                "{} is defined multiple times. Each option/command can be specified at most once.",
                arg
            );
            current = arg.to_lowercase();
            cl_args.insert(current.clone(), vec![]);
            continue;
        }

        let params = cl_args.get_mut(&current).unwrap();
        params.push(arg);
    }

    Ok((cmd, cl_args))
}

// Regular expressions used in `repair_params` function.
lazy_static! {
    // Removes whitespaces before characters: ,=()
    static ref WHITESPACE_BEFORE: Regex = Regex::new(r"\s+([,=()])").unwrap();
    // Removes whitespaces after characters: ,=(
    static ref WHITESPACE_AFTER: Regex = Regex::new(r"([,=(])\s+").unwrap();

    // Example:
    // write -schema 'replication ( factor = 3 , foo = bar )'
    // will be transformed to:
    // ["write", "-schema", "replication(factor=3,foo=bar)"]
    //
    // The reason why WHITESPACE_AFTER doesn't contain ')' character:
    // Take for example:
    // write -schema 'replication(factor=3) ' keyspace=k
    // After concatenating parameters to single string we get:
    // "write -schema replication(factor=3)  keyspace=k"
    // Note two spaces after ')'.
    // Now if we replaced ")  " with ")", the resulting vector would be:
    // ["write", "-schema", "replication(factor=3)keyspace=k"]

    // Splits the resulting arguments by whitespaces.
    static ref WHITESPACE_REGEX: Regex = Regex::new(r"\s+").unwrap();
}

/// Removes the unnecessary whitespaces from the arguments,
/// and then splits the arguments that contain whitespaces.
/// For example when user passes following arguments (cassandra-stress accepts such command):
/// read -rate 'threads=80 throttle=8000/s'
///
/// Note that 'threads=80 throttle=8000/s' will be treated as a single string,
/// so we need to split this into two separate parameters.
/// The resulting vector would in this case be:
/// ["read", "-rate", "threads=80", "throttle=8000/s"]
fn repair_params<'a, I, S>(args: I) -> Vec<String>
where
    I: Iterator<Item = &'a S>,
    S: AsRef<str> + 'a,
{
    // Concat to single string.
    let args = args.map(|s| s.as_ref()).collect::<Vec<&str>>().join(" ");

    let replaced = WHITESPACE_BEFORE.replace_all(&args, "$1");
    let replaced = WHITESPACE_AFTER.replace_all(&replaced, "$1");
    WHITESPACE_REGEX
        .split(&replaced)
        .map(&str::to_owned)
        .collect()
}

pub fn parse_cassandra_stress_args<I, S>(mut args: I) -> Result<CassandraStressParsingResult>
where
    I: Iterator<Item = S>,
    S: AsRef<str>,
{
    let _program_name = args.next().unwrap();
    let args: Vec<S> = args.collect();
    let args: Vec<String> = repair_params(args.iter());

    let result = || {
        let (cmd, mut payload) = prepare_parse_payload(&args)?;

        let (command, command_params) = match parse_command(cmd, &mut payload) {
            Ok((_, None)) => return Ok(CassandraStressParsingResult::SpecialCommand),
            Ok((cmd, Some(params))) => (cmd, params),
            Err(e) => return Err(e),
        };

        let node = NodeOption::parse(&mut payload)?;
        let rate = RateOption::parse(&mut payload)?;
        let mode = ModeOption::parse(&mut payload)?;
        let schema = SchemaOption::parse(&mut payload)?;
        let column = ColumnOption::parse(&mut payload)?;
        let log = LogOption::parse(&mut payload)?;
        let transport = TransportOption::parse(&mut payload)?;
        let errors = ErrorsOption::parse(&mut payload)?;

        // The default distribution (if not specified) is SEQ(1..operation_count).
        // If operation_count is not specified, then the default is 1M.
        let operation_count = command_params
            .common
            .operation_count
            .map_or(String::from("1000000"), |op| format!("{op}"));
        let population = PopulationOption::parse(&mut payload, &operation_count)?;

        // List the unknown options along with their parameters.
        let build_unknown_arguments_err_message = || -> String {
            let unknowns = payload
                .iter()
                .map(|(option, params)| {
                    let params_str = params.join(" ");
                    format!("{option} {params_str}")
                })
                .collect::<Vec<_>>();
            unknowns.join("\n")
        };

        // Ensure that all of the CLI arguments were consumed.
        // If not, then unknown arguments appeared so we return the error.
        anyhow::ensure!(
            payload.is_empty(),
            "Error processing CLI arguments. The following were ignored:\n{}",
            build_unknown_arguments_err_message()
        );

        Ok(CassandraStressParsingResult::Workload(Box::new(
            CassandraStressSettings {
                command,
                command_params,
                node,
                rate,
                mode,
                schema,
                column,
                population,
                log,
                transport,
                errors,
            },
        )))
    };

    match result() {
        Ok(v) => Ok(v),
        Err(e) => {
            print_help();
            Err(e)
        }
    }
}
