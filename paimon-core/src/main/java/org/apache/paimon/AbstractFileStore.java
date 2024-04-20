/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.deletionvectors.DeletionVectorsIndexFile;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.HashIndexFile;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.metastore.AddPartitionTagCallback;
import org.apache.paimon.metastore.MetastoreClient;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.operation.SnapshotDeletion;
import org.apache.paimon.operation.TagDeletion;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.service.ServiceManager;
import org.apache.paimon.stats.StatsFile;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.sink.CallbackUtils;
import org.apache.paimon.table.sink.TagCallback;
import org.apache.paimon.tag.TagAutoManager;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SegmentsCache;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.paimon.utils.BranchManager.DEFAULT_MAIN_BRANCH;

/**
 * Base {@link FileStore} implementation.
 *
 * @param <T> type of record to read and write.
 */
public abstract class AbstractFileStore<T> implements FileStore<T> {

    protected final FileIO fileIO;
    protected final SchemaManager schemaManager;
    protected final TableSchema schema;
    protected final CoreOptions options;
    protected final RowType partitionType;
    private final CatalogEnvironment catalogEnvironment;

    @Nullable private final SegmentsCache<String> writeManifestCache;

    public AbstractFileStore(
            FileIO fileIO,
            SchemaManager schemaManager,
            TableSchema schema,
            CoreOptions options,
            RowType partitionType,
            CatalogEnvironment catalogEnvironment) {
        this.fileIO = fileIO;
        this.schemaManager = schemaManager;
        this.schema = schema;
        this.options = options;
        this.partitionType = partitionType;
        this.catalogEnvironment = catalogEnvironment;
        MemorySize writeManifestCache = options.writeManifestCache();
        this.writeManifestCache =
                writeManifestCache.getBytes() == 0
                        ? null
                        : new SegmentsCache<>(options.pageSize(), writeManifestCache);
    }

    /**
     * creates a `FileStorePathFactory` instance that generates file paths based on
     * user-defined options, including the partition type and default name, as well as
     * the format identifier of the file format used by the system.
     * 
     * @returns a `FileStorePathFactory` object that creates file store paths based on
     * user-defined options.
     * 
     * 	- `options`: The options object containing configuration parameters for the file
     * store path factory.
     * 	- `path`: The base path for the file store.
     * 	- `partitionType`: The type of partitioning used for the file store (e.g., "leaf",
     * "non-leaf").
     * 	- `partitionDefaultName`: The default name of a partition in the file store, if
     * applicable.
     * 	- `fileFormat`: The file format used by the file store, which contains information
     * about the format identifier.
     */
    @Override
    public FileStorePathFactory pathFactory() {
        return new FileStorePathFactory(
                options.path(),
                partitionType,
                options.partitionDefaultName(),
                options.fileFormat().getFormatIdentifier());
    }

    /**
     * creates an instance of `SnapshotManager` using `fileIO` and the path provided by
     * `options.path()`. The resulting `SnapshotManager` object manages snapshot-related
     * operations for the application.
     * 
     * @returns an instance of the `SnapshotManager` class, which manages snapshots for
     * a given path.
     * 
     * 	- `fileIO`: This is an instance of `FileIO`, which provides methods for reading
     * and writing files.
     * 	- `path`: This is the path to the snapshot file specified by the options.
     * 	- `SnapshotManager`: This is a class that manages snapshots, providing methods
     * for creating, deleting, and retrieving snapshots.
     */
    @Override
    public SnapshotManager snapshotManager() {
        return new SnapshotManager(fileIO, options.path());
    }

    /**
     * returns a `ManifestFile.Factory` instance for creating `ManifestFile` objects with
     * the specified `true` or `false` argument indicating whether the manifest file
     * should be generated for the main class or not.
     * 
     * @returns a `ManifestFile.Factory` object that creates a new instance of `ManifestFile`.
     * 
     * 	- The `ManifestFile.Factory` object represents an interface for creating manifest
     * files.
     * 	- The `return` statement specifies that the factory should return an instance of
     * this interface.
     * 	- The `false` parameter passed to the `manifestFileFactory` function indicates
     * that the returned manifest file will not be read-only.
     */
    @Override
    public ManifestFile.Factory manifestFileFactory() {
        return manifestFileFactory(false);
    }

    /**
     * creates a `ManifestFile.Factory` instance to generate or update manifest files
     * based on input parameters such as file I/O, schema manager, partition type, and
     * options for manifest format, path factory, and target size.
     * 
     * @param forWrite whether the factory should create manifest files for write or
     * read-only access.
     * 
     * @returns a `ManifestFile.Factory` instance, which provides methods for creating
     * and manipulating manifest files.
     * 
     * 	- `fileIO`: The File IO interface for reading and writing files.
     * 	- `schemaManager`: A schema manager for managing XML schemas.
     * 	- `partitionType`: The type of partition being created (e.g., `PARTITION`).
     * 	- `options.manifestFormat()`: The manifest format to use (e.g., `MANIFEST_1_0`).
     * 	- `pathFactory()`: A path factory for generating paths.
     * 	- `options.manifestTargetSize().getBytes()`: The target size of the manifest file
     * in bytes.
     * 	- `forWrite`: A boolean indicating whether the manifest file is being created for
     * writing or reading.
     */
    protected ManifestFile.Factory manifestFileFactory(boolean forWrite) {
        return new ManifestFile.Factory(
                fileIO,
                schemaManager,
                partitionType,
                options.manifestFormat(),
                pathFactory(),
                options.manifestTargetSize().getBytes(),
                forWrite ? writeManifestCache : null);
    }

    /**
     * generates a `ManifestList.Factory` object that produces instances of `ManifestList`
     * with an empty list of manifests when the parameter `includeDefaultManifest` is set
     * to `false`.
     * 
     * @returns a `ManifestList.Factory` object that represents a factory for creating
     * instances of the `ManifestList` class with a false value for the `addToManifest`
     * method.
     * 
     * 	- The ` Factory` object returned is of type `ManifestList.Factory`.
     * 	- The `false` argument passed to the function indicates that the factory should
     * not create a new manifest list when it is used.
     */
    @Override
    public ManifestList.Factory manifestListFactory() {
        return manifestListFactory(false);
    }

    /**
     * creates a `ManifestList.Factory` instance, which is used to generate manifest
     * files, with options for writing or reading the manifest files.
     * 
     * @param forWrite whether the manifest list is being created for read or write access,
     * and it determines the cache to be used when creating the manifest list.
     * 
     * @returns a `ManifestList.Factory` object that creates and manipulates manifest
     * files for various purposes.
     * 
     * 	- `fileIO`: A reference to an interface for handling file input/output operations.
     * 	- `options.manifestFormat()`: A reference to a method that returns a manifest
     * format string.
     * 	- `pathFactory()`: A reference to a method that returns a path factory object.
     * 	- `forWrite ? writeManifestCache : null`: If `forWrite` is `true`, the
     * `writeManifestCache` value is returned, otherwise it is `null`.
     */
    protected ManifestList.Factory manifestListFactory(boolean forWrite) {
        return new ManifestList.Factory(
                fileIO,
                options.manifestFormat(),
                pathFactory(),
                forWrite ? writeManifestCache : null);
    }

    /**
     * creates a new instance of `IndexManifestFile.Factory`, which is responsible for
     * generating and managing manifest files for the application.
     * 
     * @returns an instance of `IndexManifestFile.Factory`.
     * 
     * 	- `fileIO`: This is an instance of `FileIO`, which represents the file input and
     * output operations for the manifest file.
     * 	- `manifestFormat`: This is a format string that specifies the structure of the
     * manifest file.
     * 	- `pathFactory`: This is a function that generates a path to the manifest file.
     * 
     * Overall, this function returns an instance of `IndexManifestFile.Factory`, which
     * provides a way to create and manipulate `IndexManifestFile` objects.
     */
    protected IndexManifestFile.Factory indexManifestFileFactory() {
        return new IndexManifestFile.Factory(fileIO, options.manifestFormat(), pathFactory());
    }

    /**
     * creates an instance of the `IndexFileHandler` class, which handles various aspects
     * of indexing files, including snapshot management, file manipulation, and index
     * creation using `HashIndexFile` and `DeletionVectorsIndexFile`.
     * 
     * @returns an instance of `IndexFileHandler`, which provides a way to manage and
     * manipulate index files.
     * 
     * 	- `snapshotManager()`: This represents the snapshot manager, which is responsible
     * for managing the creation and updating of snapshots.
     * 	- `pathFactory().indexFileFactory()`: This refers to the index file factory, which
     * is used to create the index files.
     * 	- `indexManifestFileFactory().create()`: This represents the index manifest file
     * factory, which creates the index manifest file.
     * 	- `new HashIndexFile(fileIO, pathFactory().indexFileFactory())`: This creates a
     * new instance of the hash index file, which is used to store the contents of the
     * index files in a compact and efficient manner.
     * 	- `new DeletionVectorsIndexFile(fileIO, pathFactory().indexFileFactory())`: This
     * creates a new instance of the deletion vectors index file, which stores information
     * about the deletion of documents in the index.
     */
    @Override
    public IndexFileHandler newIndexFileHandler() {
        return new IndexFileHandler(
                snapshotManager(),
                pathFactory().indexFileFactory(),
                indexManifestFileFactory().create(),
                new HashIndexFile(fileIO, pathFactory().indexFileFactory()),
                new DeletionVectorsIndexFile(fileIO, pathFactory().indexFileFactory()));
    }

    /**
     * creates a new instance of `StatsFileHandler`, which is responsible for handling
     * statistical data files. The function returns an instance of `StatsFileHandler`
     * that includes a `snapshotManager`, `schemaManager`, and a `StatsFile` object for
     * reading and writing statistical data to a file.
     * 
     * @returns a new instance of the `StatsFileHandler` class, which provides functionality
     * for handling statistics files.
     * 
     * 	- `snapshotManager()`: This is a reference to a SnapshotManager object, which
     * manages the creation and modification of snapshots.
     * 	- `schemaManager`: This is a reference to a SchemaManager object, which manages
     * the schema of the data being handled by the StatsFileHandler.
     * 	- `StatsFile`: This is an instance of the StatsFile class, which represents the
     * file handle for the statistics file. The `fileIO` and `pathFactory()` arguments
     * passed to the constructor provide the file input/output operations and the path
     * factory for the file.
     */
    @Override
    public StatsFileHandler newStatsFileHandler() {
        return new StatsFileHandler(
                snapshotManager(),
                schemaManager,
                new StatsFile(fileIO, pathFactory().statsFileFactory()));
    }

    /**
     * returns the `RowType` value assigned to it by its class.
     * 
     * @returns a `RowType`.
     * 
     * The RowType object is an instance of the RowType class, which represents a table
     * row in the database. The partition type determines how the data is divided and
     * stored in the table.
     * The return value of this function is always of type RowType, indicating that it
     * returns a single row of data that has been partitioned according to some scheme.
     * No additional information about the code or its author is provided beyond what is
     * strictly necessary to answer the question at hand.
     */
    @Override
    public RowType partitionType() {
        return partitionType;
    }

    /**
     * returns a `CoreOptions` object, which is an instance of a class that provides
     * access to various options related to the Java runtime environment.
     * 
     * @returns a `CoreOptions` object containing the configuration settings for the Java
     * application.
     * 
     * The `CoreOptions` object is a instance of the class `CoreOptions`.
     * It has a field named `options`, which is also of type `CoreOptions`.
     * This means that the returned output is an instance of the same class as the original
     * input.
     * The field `options` contains the original options passed to the function.
     */
    @Override
    public CoreOptions options() {
        return options;
    }

    /**
     * takes a `RowType` object and a boolean parameter, `allowExplicitCast`, and merges
     * the schema with the provided arguments. It returns a boolean value indicating
     * whether the merge was successful.
     * 
     * @param rowType type of row that is being merged.
     * 
     * RowType rowType is an object that contains attributes.
     * The `allowExplicitCast` argument indicates whether or not explicit casting can be
     * performed during schema merging.
     * The method `mergeSchema` returns a boolean value indicating whether the schema
     * merge operation was successful or not.
     * 
     * @param allowExplicitCast ability to perform an explicit cast when merging schemas.
     * 
     * @returns a boolean value indicating whether the merge operation was successful.
     */
    @Override
    public boolean mergeSchema(RowType rowType, boolean allowExplicitCast) {
        return schemaManager.mergeSchema(rowType, allowExplicitCast);
    }

    /**
     * creates a new commit object for the given user and default main branch.
     * 
     * @param commitUser user who will commit the changes to the file store.
     * 
     * @returns a `FileStoreCommitImpl` instance.
     * 
     * 1/ `FileStoreCommitImpl`: This is the type of object that is returned by the
     * function, which represents a commit in a file store.
     * 2/ `commitUser`: This is the user who committed the changes, as specified in the
     * function parameter.
     * 3/ `DEFAULT_MAIN_BRANCH`: This is the default main branch of the repository where
     * the commit takes place, as specified in the function parameter.
     */
    @Override
    public FileStoreCommitImpl newCommit(String commitUser) {
        return newCommit(commitUser, DEFAULT_MAIN_BRANCH);
    }

    /**
     * creates a new commit instance for a file store, providing various inputs such as
     * user name, branch name, and configuration options. It returns a new commit instance
     * with the required components.
     * 
     * @param commitUser user who committed the changes to the file store.
     * 
     * @param branchName name of the branch to which the commit belongs.
     * 
     * @returns a new instance of `FileStoreCommitImpl`.
     * 
     * 	- `fileIO`: Represents an object that provides file input and output operations.
     * 	- `schemaManager`: Manages schemas for data stored in the commit.
     * 	- `commitUser`: The user who committed the data.
     * 	- `partitionType`: Indicates the type of partitioning used for storing data in
     * the commit.
     * 	- `pathFactory()`: Creates paths for files and directories within the commit.
     * 	- `snapshotManager()`: Manages snapshots for the commit.
     * 	- `manifestFileFactory()`: Creates manifest files for the commit.
     * 	- `manifestListFactory()`: Creates lists of manifest files for the commit.
     * 	- `indexManifestFileFactory()`: Creates index manifest files for the commit.
     * 	- `newScan()`: Represents a new scan instance for the commit.
     * 	- `options`: Stores configuration options for the commit, including bucket,
     * manifest target size, full compaction threshold size, merge minimum count, and
     * dynamic partition overwrite.
     * 	- `keyComparator`: Compares keys for efficient data storage and retrieval within
     * the commit.
     * 	- `branchName`: The name of the branch on which the data is committed.
     * 	- `statsFileHandler`: Provides statistics related to the commit.
     */
    public FileStoreCommitImpl newCommit(String commitUser, String branchName) {
        return new FileStoreCommitImpl(
                fileIO,
                schemaManager,
                commitUser,
                partitionType,
                pathFactory(),
                snapshotManager(),
                manifestFileFactory(),
                manifestListFactory(),
                indexManifestFileFactory(),
                newScan(),
                options.bucket(),
                options.manifestTargetSize(),
                options.manifestFullCompactionThresholdSize(),
                options.manifestMergeMinCount(),
                partitionType.getFieldCount() > 0 && options.dynamicPartitionOverwrite(),
                newKeyComparator(),
                branchName,
                newStatsFileHandler());
    }

    /**
     * creates a `SnapshotDeletion` object that provides file management and metadata
     * services for snapshot deletion. It takes inputs from various factory methods and
     * returns a fully configured `SnapshotDeletion` instance.
     * 
     * @returns a `SnapshotDeletion` object containing various components for managing snapshots.
     * 
     * 	- `fileIO`: Represents the file input/output operations for snapshot deletion.
     * 	- `pathFactory()`: Creates paths for snapshot deletion.
     * 	- `manifestFileFactory().create()`: Creates a manifest file for snapshot deletion.
     * 	- `manifestListFactory().create()`: Creates a list of manifest files for snapshot
     * deletion.
     * 	- `newIndexFileHandler()`: Handles the creation of an index file for snapshot deletion.
     * 	- `newStatsFileHandler()`: Handles the creation of a stats file for snapshot deletion.
     */
    @Override
    public SnapshotDeletion newSnapshotDeletion() {
        return new SnapshotDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler());
    }

    /**
     * creates a new instance of the `TagManager` class, providing an instance of the
     * `FileIO` class and the path to the options file as parameters.
     * 
     * @returns a new instance of the `TagManager` class, which manages tags for a website.
     * 
     * 	- `fileIO`: Represents the file input/output operations for managing tags.
     * 	- `path()`: Provides the path to the location where the tags will be stored.
     * 	- The return type is `TagManager`, which indicates that it returns an instance
     * of a class that manages tags.
     */
    @Override
    public TagManager newTagManager() {
        return new TagManager(fileIO, options.path());
    }

    /**
     * creates a new instance of the `TagDeletion` class, providing various components
     * required for tag deletion, such as file I/O, path factory, manifest file, and index
     * and stats file handlers.
     * 
     * @returns a `TagDeletion` object containing various factory methods for file IO,
     * manifest files, and index and stats files.
     * 
     * 	- `fileIO`: Represents an instance of the `FileIO` class, which provides methods
     * for reading and writing files.
     * 	- `pathFactory()`: Creates an instance of a `PathFactory` class, which provides
     * methods for generating paths based on various input parameters.
     * 	- `manifestFileFactory().create()`: Returns an instance of a `ManifestFile` class,
     * which represents the manifest file for the tag deletion operation.
     * 	- `manifestListFactory().create()`: Creates an instance of a `ManifestList` class,
     * which stores the manifest files for the tag deletion operation.
     * 	- `newIndexFileHandler()`: Represents an instance of the `NewIndexFileHandler`
     * class, which provides methods for handling the index file related to the tag
     * deletion operation.
     * 	- `newStatsFileHandler()`: Represents an instance of the `NewStatsFileHandler`
     * class, which provides methods for handling the stats file related to the tag
     * deletion operation.
     */
    @Override
    public TagDeletion newTagDeletion() {
        return new TagDeletion(
                fileIO,
                pathFactory(),
                manifestFileFactory().create(),
                manifestListFactory().create(),
                newIndexFileHandler(),
                newStatsFileHandler());
    }

    public abstract Comparator<InternalRow> newKeyComparator();

    /**
     * creates a `PartitionExpire` object representing a partition expiration. It takes
     * a commit user and uses the options to determine the partition type, expiration
     * time, check interval, timestamp pattern, formatter, and scanner.
     * 
     * @param commitUser username of the user who committed the transaction that created
     * the partition, and is used to set the timestamp of the partition's expiration based
     * on the user's identity.
     * 
     * @returns a `PartitionExpire` object containing various parameters related to
     * partitioning and expiration.
     * 
     * 	- `partitionType()` represents the type of the partition being created.
     * 	- `partitionExpireTime` is the duration of the partition's expiration time.
     * 	- `partitionExpireCheckInterval` is the interval at which the partition's expiration
     * time is checked.
     * 	- `partitionTimestampPattern()` and `partitionTimestampFormatter()` define how
     * the partition's timestamp is formatted and interpreted.
     * 	- `newScan()` represents the result of a new scan operation performed on the partition.
     * 	- `newCommit(commitUser)` represents the result of a new commit operation performed
     * on the partition with the specified `commitUser`.
     */
    @Override
    @Nullable
    public PartitionExpire newPartitionExpire(String commitUser) {
        Duration partitionExpireTime = options.partitionExpireTime();
        if (partitionExpireTime == null || partitionType().getFieldCount() == 0) {
            return null;
        }

        return new PartitionExpire(
                partitionType(),
                partitionExpireTime,
                options.partitionExpireCheckInterval(),
                options.partitionTimestampPattern(),
                options.partitionTimestampFormatter(),
                newScan(),
                newCommit(commitUser));
    }

    /**
     * creates a TagAutoManager instance with options, snapshot manager, new tag manager,
     * new tag deletion, and create tag callbacks.
     * 
     * @returns an instance of `TagAutoManager`.
     * 
     * 	- `options`: an instance of `TagAutoManagerOptions` representing the configuration
     * for managing tags.
     * 	- `snapshotManager`: an instance of `SnapshotManager` providing a way to manage
     * snapshots.
     * 	- `newTagManager`: an instance of `TagManager` responsible for creating new tags.
     * 	- `newTagDeletion`: an instance of `TagDeletion` representing the ability to
     * delete tags.
     * 	- `createTagCallbacks`: an instance of `CreateTagCallbacks` providing callbacks
     * for creating new tags.
     */
    @Override
    public TagAutoManager newTagCreationManager() {
        return TagAutoManager.create(
                options,
                snapshotManager(),
                newTagManager(),
                newTagDeletion(),
                createTagCallbacks());
    }

    /**
     * creates a list of tag callbacks based on the options provided, and adds an additional
     * callback to partition the data if necessary.
     * 
     * @returns a list of tag callbacks, including an additional callback for adding
     * partition tags.
     * 
     * 	- The list of tag callbacks, which is an instance of `List<TagCallback>`. This
     * list contains all the tag callbacks that are to be executed during the ETL process.
     * 	- The `metastoreClientFactory`, which is an instance of `MetastoreClient.Factory`.
     * This variable represents the client factory used to create a metastore client,
     * which is required for adding partition tags to the data.
     * 	- The `partitionField`, which is a string representing the field that contains
     * the partition information for the data. If this variable is not null, it means
     * that partitioning is enabled for the data, and a tag callback is added to the list
     * to add the partition tag to the data.
     * 
     * In summary, the `createTagCallbacks` function returns a list of tag callbacks that
     * are used during the ETL process to perform various operations on the data, including
     * adding partition tags if enabled.
     */
    @Override
    public List<TagCallback> createTagCallbacks() {
        List<TagCallback> callbacks = new ArrayList<>(CallbackUtils.loadTagCallbacks(options));
        String partitionField = options.tagToPartitionField();
        MetastoreClient.Factory metastoreClientFactory =
                catalogEnvironment.metastoreClientFactory();
        if (partitionField != null && metastoreClientFactory != null) {
            callbacks.add(
                    new AddPartitionTagCallback(metastoreClientFactory.create(), partitionField));
        }
        return callbacks;
    }

    /**
     * creates a new instance of `ServiceManager`, which is used to manage services for
     * the application. The new instance is created by calling `new ServiceManager(fileIO,
     * options.path())`.
     * 
     * @returns a new instance of `ServiceManager` initialized with `fileIO` and the path
     * specified in the `options` object.
     * 
     * 	- The output is of type `ServiceManager`, indicating that it is a manager for services.
     * 	- The `fileIO` parameter is used to create a new instance of the `FileIO` class,
     * which provides functionality for interacting with files.
     * 	- The `options.path()` method call returns a string representing the path to the
     * directory where the services will be stored.
     */
    @Override
    public ServiceManager newServiceManager() {
        return new ServiceManager(fileIO, options.path());
    }
}
