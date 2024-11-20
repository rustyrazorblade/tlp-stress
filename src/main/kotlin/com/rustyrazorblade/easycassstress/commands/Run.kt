package com.rustyrazorblade.easycassstress.commands

import com.beust.jcommander.DynamicParameter
import com.beust.jcommander.Parameter
import com.beust.jcommander.Parameters
import com.beust.jcommander.ParametersDelegate
import com.beust.jcommander.converters.IParameterSplitter
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ScheduledReporter
import com.datastax.driver.core.*
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.HostFilterPolicy
import com.datastax.driver.core.policies.RoundRobinPolicy
import com.google.common.base.Preconditions
import com.google.common.util.concurrent.RateLimiter
import com.rustyrazorblade.easycassstress.*
import com.rustyrazorblade.easycassstress.Metrics
import com.rustyrazorblade.easycassstress.converters.ConsistencyLevelConverter
import com.rustyrazorblade.easycassstress.converters.HumanReadableConverter
import com.rustyrazorblade.easycassstress.converters.HumanReadableTimeConverter
import com.rustyrazorblade.easycassstress.delegates.PopulateOptions
import com.rustyrazorblade.easycassstress.generators.ParsedFieldFunction
import com.rustyrazorblade.easycassstress.generators.Registry
import me.tongfei.progressbar.ProgressBar
import me.tongfei.progressbar.ProgressBarStyle
import org.apache.logging.log4j.kotlin.logger
import java.io.File
import java.io.PrintStream
import java.util.*
import kotlin.concurrent.fixedRateTimer
import kotlin.concurrent.schedule
import kotlin.concurrent.thread

class NoSplitter : IParameterSplitter {
    override fun split(value: String?): MutableList<String> {
        return mutableListOf(value!!)
    }

}


/**
 * command is the original command that started the program.
 * It's used solely for logging and reporting purposes.
 */
@Parameters(commandDescription = "Run a easy-cass-stress profile")
class Run(val command: String) : IStressCommand {

    @Parameter(names = ["--host"])
    var host = System.getenv("EASY_CASS_STRESS_CASSANDRA_HOST") ?: "127.0.0.1"

    @Parameter(names = ["--port"], description = "Override the cql port. Defaults to 9042.")
    var cqlPort = 9042

    @Parameter(names = ["--username", "-U"])
    var username = "cassandra"

    @Parameter(names = ["--password", "-P"])
    var password = "cassandra"

    @Parameter(required = true)
    var profile = ""

    @Parameter(names = ["--compaction"], description = "Compaction option to use.  Double quotes will auto convert to single for convenience.  A shorthand is also available: stcs, lcs, twcs.  See the full documentation for all possibilities.")
    var compaction = ""

    @Parameter(names = ["--compression"], description = "Compression options")
    var compression = ""

    @Parameter(names = ["--keyspace"], description = "Keyspace to use")
    var keyspace = "easy_cass_stress"

    @Parameter(names = ["--id"], description = "Identifier for this run, will be used in partition keys.  Make unique for when starting concurrent runners.")
    var id = "001"

    @Parameter(names = ["--partitions", "-p"], description = "Max value of integer component of first partition key.", converter = HumanReadableConverter::class)
    var partitionValues = 1000000L

//    @Parameter(names = ["--sample", "-s"], description = "Sample Rate (0-1)")
//    var sampleRate : Double? = null // .1%..  this might be better as a number, like a million.  reasonable to keep in memory

    @Parameter(names = ["--readrate", "--reads", "-r"], description = "Read Rate, 0-1.  Workloads may have their own defaults.  Default is dependent on workload.")
    var readRate : Double? = null

    @Deprecated("Deprecated, use --rate for more predictable testing.")
    @Parameter(names = ["--concurrency", "-c"], description = "DEPRECATED.  Concurrent queries allowed.  Increase for larger clusters.  This flag is deprecated and does nothing.", converter = HumanReadableConverter::class)
    var concurrency = 100L

    @Parameter(names = ["--populate"], description = "Pre-population the DB with N rows before starting load test.", converter = HumanReadableConverter::class)
    var populate = 0L

    @Parameter(names = ["--threads", "-t"], description = "Threads to run")
    var threads = 1

    @Parameter(names = ["--iterations", "-i", "-n"], description = "Number of operations to run.", converter = HumanReadableConverter::class)
    var iterations : Long = 0
    val DEFAULT_ITERATIONS : Long = 1000000

    @Parameter(names = ["--duration", "-d"], description = "Duration of the stress test.  Expressed in format 1d 3h 15m", converter = HumanReadableTimeConverter::class)
    var duration : Long = 0

    @Parameter(names = ["-h", "--help"], description = "Show this help", help = true)
    var help = false

    @Parameter(names = ["--replication"], description = "Replication options")
    var replication = "{'class': 'SimpleStrategy', 'replication_factor':3 }"

    @DynamicParameter(names = ["--field."], description = "Override a field's data generator.  Example usage: --field.tablename.fieldname='book(100,200)'")
    var fields = mutableMapOf<String, String>()

    @Parameter(names = ["--rate"], description = "Throughput rate, accepts human numbers", converter = HumanReadableConverter::class)
    var rate = 5000L

    @Parameter(names = ["--maxrlat"], description = "Max Read Latency")
    var maxReadLatency: Long? = null

    @Parameter(names = ["--maxwlat"])
    var maxWriteLatency: Long? = null

    @Parameter(names = ["--queue"], description = "Queue Depth.  2x the rate by default.")
    var queueDepth : Long = rate * 2

    @Parameter(names = ["--drop"], description = "Drop the keyspace before starting.")
    var dropKeyspace = false

    @Parameter(names = ["--cl"], description = "Consistency level for reads/writes (Defaults to LOCAL_ONE, set custom default with EASY_CASS_STRESS_CONSISTENCY_LEVEL).", converter = ConsistencyLevelConverter::class)
    var consistencyLevel = System.getenv("EASY_CASS_STRESS_CONSISTENCY_LEVEL")?.let{ ConsistencyLevelConverter().convert(it)} ?:  ConsistencyLevel.LOCAL_ONE

    @Parameter(names = ["--scl"], description = "Serial consistency level")
    var serialConsistencyLevel = System.getenv("EASY_CASS_STRESS_SERIAL_CONSISTENCY_LEVEL")?.let{ ConsistencyLevelConverter().convert(it)} ?:  ConsistencyLevel.LOCAL_SERIAL

    @Parameter(names = ["--cql"], description = "Additional CQL to run after the schema is created.  Use for DDL modifications such as creating indexes.", splitter = NoSplitter::class)
    var additionalCQL = mutableListOf<String>()

    @Parameter(names = ["--partitiongenerator", "--pg"], description = "Method of generating partition keys.  Supports random, normal (gaussian), and sequence.")
    var partitionKeyGenerator: String = "random"

    @Parameter(names = ["--coordinatoronly", "--co"], description = "Coordinator only mode.  This will cause easy-cass-stress to round robin between nodes without tokens.  Requires using -Djoin_ring=false in cassandra-env.sh.  When using this option you must only provide a coordinator to --host.")
    var coordinatorOnlyMode = false

    @Parameter(names = ["--csv"], description = "Write metrics to this file in CSV format.")
    var csvFile = ""

    @Parameter(names = ["--paging"], description = "Override the driver's default page size.")
    var paging : Int? = null

    @Parameter(names = ["--paginate"], description = "Paginate through the entire partition before completing")
    var paginate = false

    @Parameter(names = ["--rowcache"], description = "Row cache setting")
    var rowCache = "NONE"

    @Parameter(names = ["--keycache"], description = "Key cache setting")
    var keyCache = "ALL"

    @Parameter(names = ["--prometheusport"], description = "Override the default prometheus port.  Set the default with EASY_CASS_STRESS_PROM_PORT, or set to 0 to disable.")
    var prometheusPort = System.getenv("EASY_CASS_STRESS_PROM_PORT")?.toInt() ?: 9500

    @Parameter(names = ["--ssl"], description = "Enable SSL")
    var ssl = false

    @Parameter(names = ["--ttl"], description = "Table level TTL, 0 to disable.")
    var ttl : Long = 0

    @Parameter(names = ["--dc"], description = "The data center to which requests should be sent")
    var dc : String = ""

    @DynamicParameter(names = ["--workload.", "-w."], description = "Override workload specific parameters.")
    var workloadParameters: Map<String, String> = mutableMapOf()

    @Parameter(names = ["--no-schema"], description = "Skips schema creation")
    var noSchema: Boolean = false

    @Parameter(names = ["--deleterate", "--deletes"], description = "Deletion Rate, 0-1.  Workloads may have their own defaults.  Default is dependent on workload.")
    var deleteRate : Double? = null

    val log = logger()

    @Parameter(names = ["--max-requests"], description = "Sets the max requests per connection")
    var maxRequestsPerConnection : Int = 32768

    @Parameter(names = ["--core-connections"], description = "Sets the number of core connections per host")
    var coreConnections : Int = 4

    @Parameter(names = ["--max-connections"], description = "Sets the number of max connections per host")
    var maxConnections : Int = 8

    @Parameter(names = ["--hdr"], description = "Print HDR Histograms using this prefix")
    var hdrHistogramPrefix = ""

    @ParametersDelegate
    var populateRate = PopulateOptions()

    /**
     * Lazily generate query options
     */
    val options by lazy {
        val tmp = QueryOptions().setConsistencyLevel(consistencyLevel).setSerialConsistencyLevel(serialConsistencyLevel)
        if(paging != null) {
            println("Using custom paging size of $paging")
            tmp.setFetchSize(paging!!)
        }
        tmp
    }

    val session by lazy {

        var builder = Cluster.builder()
                .addContactPoint(host)
                .withPort(cqlPort)
                .withCredentials(username, password)
                .withQueryOptions(options)
                .withPoolingOptions(PoolingOptions()
                        .setConnectionsPerHost(HostDistance.LOCAL, coreConnections, maxConnections)
                        .setConnectionsPerHost(HostDistance.REMOTE, coreConnections, maxConnections)
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection)
                        .setMaxRequestsPerConnection(HostDistance.REMOTE, maxRequestsPerConnection))
        if(ssl) {
            builder = builder.withSSL()
        }

        if (dc != "") {
            builder.withLoadBalancingPolicy(DCAwareRoundRobinPolicy.builder()
                    .withLocalDc(dc)
                    .withUsedHostsPerRemoteDc(0)
                    .build())
        }

        if(coordinatorOnlyMode) {
            println("Using experimental coordinator only mode.")
            val policy = HostFilterPolicy(RoundRobinPolicy(), CoordinatorHostPredicate())
            builder = builder.withLoadBalancingPolicy(policy)
        }

        val cluster = builder.build()

        // get all the initial schema
        println("Creating schema")

        println("Executing $iterations operations with consistency level $consistencyLevel and serial consistency level $serialConsistencyLevel")

        val session = cluster.connect()

        // println("Connected")
        println("Connected to Cassandra cluster.")
        session
    }


    override fun execute() {

        Preconditions.checkArgument(!(duration > 0 && iterations > 0L), "Duration and iterations shouldn't be both set at the same time. Please pick just one.")
        iterations = if (duration == 0L && iterations == 0L) DEFAULT_ITERATIONS else iterations // apply the default if the number of iterations wasn't set


        Preconditions.checkArgument(partitionKeyGenerator in setOf("random", "normal", "sequence"), "Partition generator Supports random, normal, and sequence.")

        val plugin = Plugin.getPlugins().get(profile) ?: error("$profile profile does not exist")

        // Check the workload parameters exist in the workload before we start testing.
        workloadParameters.forEach {(key) -> (
            try {
                plugin.getProperty(key)
            }
            catch (nsee: java.util.NoSuchElementException) {
                error("Workload $profile has no parameter '$key'. Please run the 'info' command to see the list of available parameters for this workload.")
            }
        )}

        /**
         * Check to make sure that readRate + deleteRate <= 1.0 before we start testing.
         *
         * Resolve the final value for the readRate and deleteRate, then check their sum. Note that the readRate is supplied
         * by the profile when the commandline option is unspecified.
         */
        val tmpReadRate = readRate ?: plugin.instance.getDefaultReadRate()
        val tmpDeleteRate = deleteRate ?: 0.0

        if ((tmpReadRate + tmpDeleteRate) > 1.0) {
            error("The readRate + deleteRate must be <= 1.0. Values supplied were: readRate = $tmpReadRate and deleteRate = $tmpDeleteRate.")
        }

        createKeyspace()

        val rateLimiter = getRateLimiter()

        plugin.applyDynamicSettings(workloadParameters)

        createSchema(plugin)
        executeAdditionalCQL()

        val fieldRegistry = createFieldRegistry(plugin)

        println("Preparing queries")
        plugin.instance.prepare(session)

        // Both of the following are set in the try block, so this is OK
        val metrics = createMetrics()

        // set up the rate limiter optimizer and put it on a schedule
        // var optimizer = RateLimiterOptimizer(rateLimiter, metrics, maxReadLatency, maxWriteLatency)
        // optimizer.reset()

        // val timer = Timer().schedule(10000, 5000) {
        //     optimizer.execute()
        // }

        var runnersExecuted = 0L

        try {
            // run the prepare for each
            val runners = createRunners(plugin, metrics, fieldRegistry, rateLimiter)

            rateLimiter.rate = populateRate.rate

            populateData(plugin, runners, metrics)

            rateLimiter.rate = rate.toDouble()

            metrics.resetErrors()

            // since the populate workload is going to be substantially different
            // from the test workload, we need to reset the optimizer
            // optimizer.reset()

            metrics.startReporting()
            println("Prometheus metrics are available at http://localhost:$prometheusPort/")

            println("Starting main runner")

            val threads = mutableListOf<Thread>()
            for (runner in runners) {
                val t = thread(start = true, isDaemon = true) {
                    runner.run()
                }
                runnersExecuted++

                threads.add(t)
            }

            for (t in threads) {
                t.join()
            }

            Thread.sleep(1000)

            // dump out metrics
            for (reporter in metrics.reporters) {
                reporter.report()
            }

            // print out the hdr histograms if requested to 3 separate files
            if (hdrHistogramPrefix != "") {
                val pairs = listOf(Pair(metrics.mutationHistogram, "mutations"),
                                   Pair(metrics.selectHistogram, "reads"),
                                   Pair(metrics.deleteHistogram, "deletes"))
                for (entry in pairs) {
                    val fp = File(hdrHistogramPrefix + "-" + entry.second + ".txt")
                    entry.first.outputPercentileDistribution(PrintStream(fp), 1_000_000.0)
                }
            }
        } catch (e: Exception) {
            println("There was an error with easy-cass-stress.  Please file a bug at https://github.com/rustyrazorblade/easy-cass-stress and report the following exception:\n $e")
            throw e
        } finally {
            // we need to be able to run multiple tests in the same JVM
            // without this cleanup we could have the metrics runner still running and it will cause subsequent tests to fail
            metrics.shutdown()
            Thread.sleep(1000)

            println("Stress complete, $runnersExecuted.")
        }

    }


    private fun getRateLimiter() : RateLimiter {
        val tmp = RateLimiter.create(rate.toDouble())
        tmp.acquire(rate.toInt())
        return tmp
    }


    /**
     * When we run populate, certain workloads (locking) need to do special things.
     * Like pre-fill every partition with a known dataset.
     * So we have to override populate (and in the case of locking the partition generator).
     * This allows us to do things like sequentially fill every row in the addressable
     * partition space with initial values.
     */
    private fun populateData(plugin: Plugin, runners: List<ProfileRunner>, metrics: Metrics) {
        // The --populate flag can be overridden by the profile

        val option = plugin.instance.getPopulateOption(this)
        var deletes = true

        val max = when(option) {
            is PopulateOption.Standard -> {
                populate
            }
            is PopulateOption.Custom -> {
                println("Using workload specific populate options of ${option.rows}")
                deletes = option.deletes
                if (!deletes) {
                    println("Not doing deletes in populate phase")
                }
                option.rows
            }
        }

        println("Prepopulating data with $max records per thread ($threads)")

        if(max > 0) {
            ProgressBar("Populate Progress", max * threads, ProgressBarStyle.ASCII).use {
                // update the timer every second, starting 1 second from now, as a daemon thread
                val timer = fixedRateTimer("progress-bar", true, 1000, 1000) {
                    it.stepTo(metrics.populate.count)
                }

                // calling it on the runner
                val threads = mutableListOf<Thread>()

                runners.forEach {
                    val tmp = thread(start = true, isDaemon = false, name = "populate-X") {
                        it.populate(max, deletes)
                    }
                    threads.add(tmp)
                }
                Thread.sleep(1000)
                for (thread in threads) {
                    thread.join()
                }
                // have we really reached 100%?
                Thread.sleep(1000)
                // stop outputting the progress bar
                timer.cancel()
                // allow the time to die out

            }
            Thread.sleep(1000)
            println("\nPre-populate complete.")
        }

    }

    private fun createRunners(plugin: Plugin, metrics: Metrics, fieldRegistry: Registry, rateLimiter: RateLimiter?): List<ProfileRunner> {
        val runners = IntRange(0, threads - 1).map {
            // println("Connecting")
            println("Connecting to Cassandra cluster ...")
            val context = StressContext(session, this, it, metrics, fieldRegistry, rateLimiter)
            ProfileRunner.create(context, plugin.instance)
        }

        val executed = runners.parallelStream().map {
            println("Preparing statements.")
            it.prepare()
        }.count()

        println("$executed threads prepared.")
        return runners
    }

    private fun createMetrics(): Metrics {
        println("Initializing metrics")
        val registry = MetricRegistry()

        val reporters = mutableListOf<ScheduledReporter>()

        if(csvFile.isNotEmpty()) {
            reporters.add(FileReporter(registry, csvFile, command))
        }
        reporters.add(SingleLineConsoleReporter(registry))

        return Metrics(registry, reporters, prometheusPort)
    }

    private fun createFieldRegistry(plugin: Plugin): Registry {

        val fieldRegistry = Registry.create()

        for((field,generator) in plugin.instance.getFieldGenerators()) {
            log.info("Defaulting $field to $generator")
            fieldRegistry.setDefault(field, generator)
        }

        // Fields  is the raw --fields
        for((field, generator) in fields) {
            println("$field, $generator")
            // Parsed field, switch this to use the new parser
            //val instance = Registry.getInstance(generator)
            val instance = ParsedFieldFunction(generator)

            val parts = field.split(".")
            val table = parts[0]
            val fieldName = parts[1]
//            val
            // TODO check to make sure the fields exist
            fieldRegistry.setOverride(table, fieldName, instance)
        }
        return fieldRegistry
    }

    private fun createKeyspace() {
        if(noSchema) {
            println("Skipping keyspace creation")
        }
        else {
            if (dropKeyspace) {
                println("Dropping $keyspace")
                session.execute("DROP KEYSPACE IF EXISTS $keyspace")
                Thread.sleep(5000)
            }

            val createKeyspace = """CREATE KEYSPACE
            | IF NOT EXISTS $keyspace
            | WITH replication = $replication""".trimMargin()

            println("Creating $keyspace: \n$createKeyspace\n")
            session.execute(createKeyspace)
        }
        session.execute("USE $keyspace")
    }


    private fun executeAdditionalCQL() {
        // run additional CQL
        for (statement in additionalCQL) {
            println(statement)
            session.execute(statement)
        }
    }

    fun createSchema(plugin: Plugin) {
        println("Creating Tables")

        if(noSchema) return

        for (statement in plugin.instance.schema()) {
            val s = SchemaBuilder.create(statement)
                    .withCompaction(compaction)
                    .withCompression(compression)
                    .withRowCache(rowCache)
                    .withKeyCache(keyCache)
                    .withDefaultTTL(ttl)
                    .build()
            println(s)
            session.execute(s)
        }
    }


}
