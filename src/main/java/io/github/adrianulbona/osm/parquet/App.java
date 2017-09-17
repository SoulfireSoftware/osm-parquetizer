package io.github.adrianulbona.osm.parquet;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import crosby.binary.osmosis.OsmosisReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.Files.newInputStream;
import static java.util.Collections.unmodifiableList;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Node;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Relation;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        final MultiEntitySinkConfig config = new MultiEntitySinkConfig();
        final CmdLineParser cmdLineParser = new CmdLineParser(config);
        try {
            cmdLineParser.parseArgument(args);

            try (InputStream sourceInput = loadSource(config.sourceFile)) {
                final OsmosisReader reader = new OsmosisReader(sourceInput);
                final MultiEntitySink sink = new MultiEntitySink(config);
                sink.addObserver(new MultiEntitySinkObserver());
                reader.setSink(sink);
                reader.run();
            }
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            System.out.print("Usage: java -jar osm-parquetizer.jar");
            System.out.println();
            cmdLineParser.printSingleLineUsage(System.out);
        }
    }


    private static InputStream loadSource(URI sourceFile) throws Exception {
        if (Objects.equals(sourceFile.getScheme(), "s3")) {
            String bucketName = sourceFile.getHost();
            String sourcePath = sourceFile.getPath().replaceAll("^/", "");
            log.info("Loading source file (S3): bucket=" + bucketName + ", path=" + sourcePath);

            AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
            return s3.getObject(bucketName, sourcePath).getObjectContent();
        } else if (sourceFile.getScheme() != null) {
            log.info("Loading source file (Hadoop): " + sourceFile);

            Path sourceFilePath = new Path(sourceFile.toString());
            return sourceFilePath.getFileSystem(new Configuration()).open(sourceFilePath);
        } else {
            log.info("Loading source file (Local): " + sourceFile);

            return newInputStream(Paths.get(sourceFile.toString()));
        }
    }

    private static class MultiEntitySinkConfig implements MultiEntitySink.Config {

        @Argument(index = 0, metaVar = "pbf-path", usage = "the OSM PBF file path or URI to be parquetized", required = true)
        private URI sourceFile;

        @Argument(index = 1, metaVar = "output-path", usage = "the directory path or URI where to store the Parquet files")
        private URI destinationFolder;

        @Option(name = "--exclude-metadata", usage = "if present the metadata will not be parquetized")
        private boolean excludeMetadata = false;

        @Option(name = "--no-nodes", usage = "if present the nodes will be not parquetized")
        private boolean noNodes = false;

        @Option(name = "--no-ways", usage = "if present the ways will be not parquetized")
        private boolean noWays = false;

        @Option(name = "--no-relations", usage = "if present the relations will not be parquetized")
        private boolean noRelations = false;

        @Override
        public boolean getExcludeMetadata() {
            return this.excludeMetadata;
        }

        @Override
        public URI getSourceFile() {
            return sourceFile;
        }

        @Override
        public URI getDestinationFolder() {
            return destinationFolder != null ? destinationFolder : getParentDirectory(sourceFile);
        }

        @Override
        public List<EntityType> entitiesToBeParquetized() {
            final List<EntityType> entityTypes = new ArrayList<>();
            if (!noNodes) {
                entityTypes.add(Node);
            }
            if (!noWays) {
                entityTypes.add(EntityType.Way);
            }
            if (!noRelations) {
                entityTypes.add(Relation);
            }
            return unmodifiableList(entityTypes);
        }

        private static URI getParentDirectory(URI uri) {
            try {
                return new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(),
                        FilenameUtils.getFullPath(uri.getPath()), uri.getQuery(), uri.getFragment());
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid source URI", e);
            }
        }

    }


    private static class MultiEntitySinkObserver implements MultiEntitySink.Observer {

        private final Logger log = LoggerFactory.getLogger(getClass());

        private AtomicLong totalEntitiesCount;

        @Override
        public void started() {
            totalEntitiesCount = new AtomicLong();
        }

        @Override
        public void processed(Entity entity) {
            final long count = totalEntitiesCount.incrementAndGet();
            if (count % 1000000 == 0) {
                log.info("Entities processed: " + count);

            }
        }

        @Override
        public void ended() {
            log.info("Total entities processed: " + totalEntitiesCount.get());
        }
    }
}
