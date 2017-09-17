package io.github.adrianulbona.osm.parquet;

import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.hadoop.ParquetWriter;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.String.format;


public class ParquetSink<T extends Entity> implements Sink {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final boolean excludeMetadata;
    private final EntityType entityType;
    private final List<Predicate<T>> filters;

    private final String destination;

    private ParquetWriter<T> writer;

    public ParquetSink(URI sourceFile, URI destinationFolder, boolean excludeMetadata, EntityType entityType) {
        this.excludeMetadata = excludeMetadata;
        this.entityType = entityType;
        this.filters = new ArrayList<>();

        String pbfName = FilenameUtils.getBaseName(sourceFile.getPath());
        String entityName = entityType.name().toLowerCase();
        destination = destinationFolder.resolve(format("%s.%s.parquet", pbfName, entityName)).toString();
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        try {
            log.info("Initializing ParquetWriter: entityType=" + entityType + ", destination=" + destination);

            this.writer = ParquetWriterFactory.buildFor(destination, excludeMetadata, entityType);
        } catch (IOException e) {
            throw new RuntimeException("Unable to build writers", e);
        }
    }

    @Override
    public void process(EntityContainer entityContainer) {
        try {
            if (this.entityType == entityContainer.getEntity().getType()) {
                final T entity = (T) entityContainer.getEntity();
                if (filters.stream().noneMatch(filter -> filter.test(entity))) {
                    writer.write(entity);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to write entity", e);
        }
    }

    @Override
    public void complete() {
        try {
            this.writer.close();

            log.info("Finished writing to " + destination);
        } catch (IOException e) {
            throw new RuntimeException("Unable to close writers", e);
        }
    }

    @Override
    public void release() {

    }

    public void addFilter(Predicate<T> predicate) {
        this.filters.add(predicate);
    }

    public void removeFilter(Predicate<T> predicate) {
        this.filters.remove(predicate);
    }
}
