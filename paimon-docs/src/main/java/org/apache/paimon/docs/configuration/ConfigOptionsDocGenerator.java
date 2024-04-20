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

package org.apache.paimon.docs.configuration;

import org.apache.paimon.annotation.ConfigGroup;
import org.apache.paimon.annotation.ConfigGroups;
import org.apache.paimon.annotation.Documentation;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.description.DescribedEnum;
import org.apache.paimon.options.description.Description;
import org.apache.paimon.options.description.Formatter;
import org.apache.paimon.options.description.HtmlFormatter;
import org.apache.paimon.options.description.InlineElement;
import org.apache.paimon.options.description.TextElement;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.ThrowingConsumer;
import org.apache.paimon.utils.TimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.docs.util.Utils.apacheHeaderToHtml;
import static org.apache.paimon.docs.util.Utils.escapeCharacters;
import static org.apache.paimon.options.description.TextElement.text;

/** Class used for generating code based documentation of configuration parameters. */
public class ConfigOptionsDocGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigOptionsDocGenerator.class);

    static final OptionsClassLocation[] LOCATIONS =
            new OptionsClassLocation[] {
                new OptionsClassLocation("paimon-common", "org.apache.paimon.options"),
                new OptionsClassLocation("paimon-common", "org.apache.paimon"),
                new OptionsClassLocation("paimon-core", "org.apache.paimon.lookup"),
                new OptionsClassLocation("paimon-core", "org.apache.paimon.catalog"),
                new OptionsClassLocation("paimon-core", "org.apache.paimon.jdbc"),
                new OptionsClassLocation("paimon-format", "org.apache.paimon.format"),
                new OptionsClassLocation(
                        "paimon-flink/paimon-flink-common", "org.apache.paimon.flink"),
                new OptionsClassLocation(
                        "paimon-flink/paimon-flink-cdc", "org.apache.paimon.flink.kafka"),
                new OptionsClassLocation(
                        "paimon-hive/paimon-hive-catalog", "org.apache.paimon.hive"),
                new OptionsClassLocation(
                        "paimon-spark/paimon-spark-common", "org.apache.paimon.spark")
            };
    static final String DEFAULT_PATH_PREFIX = "src/main/java";

    private static final String CLASS_NAME_GROUP = "className";
    private static final String CLASS_PREFIX_GROUP = "classPrefix";
    private static final Pattern CLASS_NAME_PATTERN =
            Pattern.compile(
                    "(?<"
                            + CLASS_NAME_GROUP
                            + ">(?<"
                            + CLASS_PREFIX_GROUP
                            + ">[a-zA-Z]*)(?:Options|Config|Parameters))(?:\\.java)?");

    private static final Formatter formatter = new HtmlFormatter();

    /**
     * This method generates html tables from set of classes containing {@link ConfigOption
     * ConfigOptions}.
     *
     * <p>For each class 1 or more html tables will be generated and placed into a separate file,
     * depending on whether the class is annotated with {@link ConfigGroups}. The tables contain the
     * key, default value and description for every {@link ConfigOption}.
     *
     * <p>One additional table is generated containing all {@link ConfigOption ConfigOptions} that
     * are annotated with {@link Documentation.Section}.
     *
     * @param args [0] output directory for the generated files [1] project root directory
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String outputDirectory = args[0];
        String rootDir = args[1];
        LOG.info(
                "Searching the following locations; configured via {}#LOCATIONS:{}",
                ConfigOptionsDocGenerator.class.getCanonicalName(),
                Arrays.stream(LOCATIONS)
                        .map(OptionsClassLocation::toString)
                        .collect(Collectors.joining("\n\t", "\n\t", "")));
        for (OptionsClassLocation location : LOCATIONS) {
            createTable(rootDir, location.getModule(), location.getPackage(), outputDirectory);
        }

        generateCommonSection(rootDir, outputDirectory, LOCATIONS, DEFAULT_PATH_PREFIX);
    }

    /**
     * groups options by section and sorts them based on their position within that
     * section. It then writes the sorted options to a HTML table in a file located in
     * the specified output directory.
     * 
     * @param rootDir base directory where the documentation will be generated.
     * 
     * @param outputDirectory directory where the generated HTML table will be written
     * to by the function.
     * 
     * @param locations 0-based index of an OptionsClassLocation object, which contains
     * metadata about a particular option within a module or package, and is used to
     * iterate through all options for each location in the code generation process.
     * 
     * 	- Each `OptionsClassLocation` object in `locations` represents an annotation on
     * a class or method in a Java module.
     * 	- The `module`, `package`, and `pathPrefix` fields of each `OptionsClassLocation`
     * represent the module, package name, and prefix of the output directory for that
     * location's options, respectively.
     * 	- The `field` field of each `OptionsClassLocation` represents the field on which
     * the annotation is placed. This field is used to determine the section of the
     * documentation to display for each option.
     * 	- The `Annotation` object returned by `getField()` represents the annotation on
     * the field, which can include information such as the documentation section and
     * position within that section.
     * 
     * @param pathPrefix prefix to be added to the output directory for each section's
     * HTML table file name, allowing for customization of the output structure.
     */
    @VisibleForTesting
    static void generateCommonSection(
            String rootDir,
            String outputDirectory,
            OptionsClassLocation[] locations,
            String pathPrefix)
            throws IOException, ClassNotFoundException {
        List<OptionWithMetaInfo> allSectionOptions = new ArrayList<>(32);
        for (OptionsClassLocation location : locations) {
            allSectionOptions.addAll(
                    findSectionOptions(
                            rootDir, location.getModule(), location.getPackage(), pathPrefix));
        }

        Map<String, List<OptionWithMetaInfo>> optionsGroupedBySection =
                allSectionOptions.stream()
                        .flatMap(
                                option -> {
                                    final String[] sections =
                                            option.field
                                                    .getAnnotation(Documentation.Section.class)
                                                    .value();
                                    if (sections.length == 0) {
                                        throw new RuntimeException(
                                                String.format(
                                                        "Option %s is annotated with %s but the list of sections is empty.",
                                                        option.option.key(),
                                                        Documentation.Section.class
                                                                .getSimpleName()));
                                    }

                                    return Arrays.stream(sections)
                                            .map(section -> Pair.of(section, option));
                                })
                        .collect(
                                Collectors.groupingBy(
                                        Pair::getLeft,
                                        Collectors.mapping(Pair::getRight, Collectors.toList())));

        optionsGroupedBySection.forEach(
                (section, options) -> {
                    options.sort(
                            (o1, o2) -> {
                                int position1 =
                                        o1.field
                                                .getAnnotation(Documentation.Section.class)
                                                .position();
                                int position2 =
                                        o2.field
                                                .getAnnotation(Documentation.Section.class)
                                                .position();
                                if (position1 == position2) {
                                    return o1.option.key().compareTo(o2.option.key());
                                } else {
                                    return Integer.compare(position1, position2);
                                }
                            });

                    String sectionHtmlTable = toHtmlTable(options);
                    try {
                        Files.write(
                                Paths.get(outputDirectory, getSectionFileName(section)),
                                (apacheHeaderToHtml() + sectionHtmlTable)
                                        .getBytes(StandardCharsets.UTF_8));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    /**
     * generates a section file name based on the inputted section string, appending an
     * underscore and the word "section" to produce the final file name.
     * 
     * @param section name of the section for which the file name is to be generated.
     * 
     * @returns a string representing the file name for a specific section, constructed
     * by combining the section name with the suffix "_section.html".
     */
    @VisibleForTesting
    static String getSectionFileName(String section) {
        return section + "_section.html";
    }

    /**
     * finds all option with meta-info within a given package and sub-package, based on
     * documentation annotations.
     * 
     * @param rootDir directory where configuration files are located.
     * 
     * @param module module name for which options are to be found.
     * 
     * @param packageName package name of the module being searched for options.
     * 
     * @param pathPrefix prefix for the path of the options files to be processed, which
     * is used to filter the options based on their paths.
     * 
     * @returns a collection of `OptionWithMetaInfo` objects that represent configuration
     * options with documentation for a specified module, package name, and path prefix.
     * 
     * 	- The Collection is of type `OptionWithMetaInfo`.
     * 	- It contains options that have a field annotated with `@Documentation.Section`.
     * 	- The options are filtered based on the value of the `pathPrefix` parameter.
     * 	- The list of options is processed using the `processConfigOptions` method, which
     * takes the root directory, module name, package name, and option class as parameters.
     */
    private static Collection<OptionWithMetaInfo> findSectionOptions(
            String rootDir, String module, String packageName, String pathPrefix)
            throws IOException, ClassNotFoundException {
        Collection<OptionWithMetaInfo> commonOptions = new ArrayList<>(32);
        processConfigOptions(
                rootDir,
                module,
                packageName,
                pathPrefix,
                optionsClass ->
                        extractConfigOptions(optionsClass).stream()
                                .filter(
                                        optionWithMetaInfo ->
                                                optionWithMetaInfo.field.getAnnotation(
                                                                Documentation.Section.class)
                                                        != null)
                                .forEachOrdered(commonOptions::add));
        return commonOptions;
    }

    /**
     * generates HTML documentation tables for a given Java class based on its configuration
     * options, and writes them to a specified output directory.
     * 
     * @param rootDir directory where the configuration files will be generated.
     * 
     * @param module module name for which the configuration options are being generated.
     * 
     * @param packageName name of the package to which the generated configuration files
     * will belong, and is used to construct the output file names in the `toSnakeCase()`
     * method.
     * 
     * @param outputDirectory directory where the generated HTML files will be saved.
     */
    private static void createTable(
            String rootDir, String module, String packageName, String outputDirectory)
            throws IOException, ClassNotFoundException {
        processConfigOptions(
                rootDir,
                module,
                packageName,
                ConfigOptionsDocGenerator.DEFAULT_PATH_PREFIX,
                optionsClass -> {
                    List<Pair<ConfigGroup, String>> tables = generateTablesForClass(optionsClass);
                    for (Pair<ConfigGroup, String> group : tables) {
                        String name;
                        if (group.getLeft() == null) {
                            Matcher matcher =
                                    CLASS_NAME_PATTERN.matcher(optionsClass.getSimpleName());
                            if (!matcher.matches()) {
                                throw new RuntimeException(
                                        "Pattern did not match for "
                                                + optionsClass.getSimpleName()
                                                + '.');
                            }
                            name = matcher.group(CLASS_PREFIX_GROUP);
                        } else {
                            name = group.getLeft().name();
                        }
                        String outputFile = toSnakeCase(name) + "_configuration.html";
                        Files.write(
                                Paths.get(outputDirectory, outputFile),
                                (apacheHeaderToHtml() + group.getRight())
                                        .getBytes(StandardCharsets.UTF_8));
                    }
                });
    }

    /**
     * converts a given string to lowercase and replaces any letter followed by an uppercase
     * letter with an underscore and the corresponding lowercase letter, resulting in a
     * snake case representation of the input string.
     * 
     * @param name string to be converted to snake case.
     * 
     * @returns a lowercase string with underscores separating words, obtained by replacing
     * and then removing the first letter of each word followed by an uppercase letter,
     * and then replacing the entire sequence with an underscore.
     */
    @VisibleForTesting
    static String toSnakeCase(String name) {
        return name.replaceAll("(.)([A-Z][a-z])", "$1_$2").toLowerCase();
    }

    /**
     * processes a configuration directory containing class files, and passes each class
     * to a consumer for further processing.
     * 
     * @param rootDir directory where the configuration files are located.
     * 
     * @param module module name associated with the configuration files to be processed,
     * which is used to determine the directory path for the configuration files.
     * 
     * @param packageName name of the package that contains the classes to be processed,
     * and is used as a prefix for the file names to determine the class names to be processed.
     * 
     * @param pathPrefix subdirectory path within the configuration directory where the
     * classes are searched for in the DirectoryStream.
     * 
     * @param classConsumer method that takes a class loader and performs some operation
     * on it, such as loading a class from the given path prefix.
     * 
     * 	- `ThrowingConsumer<Class<?>, IOException>`: This is a type-safe consumer that
     * takes a class as an input and can throw an `IOException`.
     * 	- `Class<?>`: The type of the class being consumed.
     * 	- `IOException`: The exception type that can be thrown by the consumer.
     * 	- `Path`: The path to the configuration file being processed.
     * 
     * The function takes these properties as inputs and performs operations on them,
     * such as reading from a directory stream and passing the classes to the consumer
     * for processing.
     */
    @VisibleForTesting
    static void processConfigOptions(
            String rootDir,
            String module,
            String packageName,
            String pathPrefix,
            ThrowingConsumer<Class<?>, IOException> classConsumer)
            throws IOException, ClassNotFoundException {
        Path configDir = Paths.get(rootDir, module, pathPrefix, packageName.replaceAll("\\.", "/"));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(configDir)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                Matcher matcher = CLASS_NAME_PATTERN.matcher(fileName);
                if (matcher.matches()) {
                    classConsumer.accept(
                            Class.forName(packageName + '.' + matcher.group(CLASS_NAME_GROUP)));
                }
            }
        }
    }

    /**
     * generates HTML tables representing configuration options for a given class, based
     * on annotations and extracted options. It returns a list of pairs consisting of a
     * ConfigGroup and its corresponding table.
     * 
     * @param optionsClass class that contains the configuration options to be extracted
     * and summarized.
     * 
     * 	- `ConfigGroups`: This is an annotation on the class that provides information
     * about the configuration groups defined in the class. The `groups()` method returns
     * a list of configuration group names.
     * 	- `allOptions`: This is a list of `OptionWithMetaInfo` objects that represent all
     * the configuration options available for the class.
     * 	- `configGroups`: If this is not null, it indicates that there are defined
     * configuration groups in the class, and the function creates tables for each group
     * separately. The `groups()` method returns a list of configuration group names.
     * 	- `tree`: This is an instance of the `Tree` class, which represents the hierarchy
     * of configuration options based on their grouping. The `findConfigOptions()` method
     * finds all the configuration options that belong to a given configuration group.
     * 	- `sortOptions`: This is a method that sorts the configuration options in each
     * group alphabetically.
     * 	- `toHtmlTable`: This is a method that converts the sorted list of configuration
     * options into an HTML table.
     * 
     * @returns a list of pairs containing a ConfigGroup and its corresponding HTML table
     * representation.
     * 
     * 	- `tables`: A list of pairs containing the ConfigGroup and the corresponding HTML
     * table for each option in the class. If the ConfigGroups annotation is present, the
     * list will have length equal to the number of ConfigGroups, followed by a single
     * entry with null as the ConfigGroup and the HTML table for the default options.
     * Otherwise, the list will have only one entry with null as the ConfigGroup and the
     * HTML table for all options.
     * 	- `Pair`: The type of the elements in the `tables` list, which are pairs containing
     * the ConfigGroup and the HTML table for each option.
     */
    @VisibleForTesting
    static List<Pair<ConfigGroup, String>> generateTablesForClass(Class<?> optionsClass) {
        ConfigGroups configGroups = optionsClass.getAnnotation(ConfigGroups.class);
        List<OptionWithMetaInfo> allOptions = extractConfigOptions(optionsClass);
        if (allOptions.isEmpty()) {
            return Collections.emptyList();
        }
        List<Pair<ConfigGroup, String>> tables;
        if (configGroups != null) {
            tables = new ArrayList<>(configGroups.groups().length + 1);
            Tree tree = new Tree(configGroups.groups(), allOptions);
            for (ConfigGroup group : configGroups.groups()) {
                List<OptionWithMetaInfo> configOptions = tree.findConfigOptions(group);
                if (!configOptions.isEmpty()) {
                    sortOptions(configOptions);
                    tables.add(Pair.of(group, toHtmlTable(configOptions)));
                }
            }
            List<OptionWithMetaInfo> configOptions = tree.getDefaultOptions();
            if (!configOptions.isEmpty()) {
                sortOptions(configOptions);
                tables.add(Pair.of(null, toHtmlTable(configOptions)));
            }
        } else {
            sortOptions(allOptions);
            tables = Collections.singletonList(Pair.of(null, toHtmlTable(allOptions)));
        }
        return tables;
    }

    /**
     * extracts configuration options from a given class by iterating through its fields,
     * checking if they are config options and should be documented, and adding them to
     * a list.
     * 
     * @param clazz Class object for which config options are to be extracted.
     * 
     * 	- `clazz`: The class to be analyzed for config options.
     * 	- `fields`: An array of fields within the `clazz`.
     * 	- `isConfigOption`: A method that determines whether a field is a config option
     * or not.
     * 	- `shouldBeDocumented`: Another method that decides if a field should be documented
     * or not.
     * 
     * The function then iterates through each field in the `fields` array and applies
     * the `isConfigOption` and `shouldBeDocumented` methods to determine which fields
     * are config options and should be included in the list of config options. The
     * resulting list of config options is returned at the end of the function.
     * 
     * @returns a list of `OptionWithMetaInfo` objects representing configured fields in
     * a class.
     * 
     * 	- The list of config options is generated using the `getFields` method of the
     * class, which returns a list of all fields in the class.
     * 	- Each field is checked if it is a `ConfigOption` object using the `isConfigOption`
     * method. If it is, the field's value is obtained using the `get(null)` method, and
     * the resulting `OptionWithMetaInfo` object is added to the list of config options.
     * 	- The `shouldBeDocumented` method is called on each field to determine if it
     * should be documented. If the method returns `true`, the field is included in the
     * list of config options.
     * 	- The list of config options returned by the function is a list of `OptionWithMetaInfo`
     * objects, where each object represents a single config option with its associated
     * metadata.
     */
    @VisibleForTesting
    static List<OptionWithMetaInfo> extractConfigOptions(Class<?> clazz) {
        try {
            List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            Field[] fields = clazz.getFields();
            for (Field field : fields) {
                if (isConfigOption(field) && shouldBeDocumented(field)) {
                    configOptions.add(
                            new OptionWithMetaInfo((ConfigOption<?>) field.get(null), field));
                }
            }
            return configOptions;
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to extract config options from class " + clazz + '.', e);
        }
    }

    /**
     * determines if a given Field represents a ConfigOption type. It returns `true` if
     * the Field's type is equal to `ConfigOption.class`, and `false` otherwise.
     * 
     * @param field Field object to be checked for being a ConfigOption, allowing the
     * function to determine if it is a ConfigOption or not.
     * 
     * 	- The type of the field is checked to be equal to `ConfigOption.class`, indicating
     * that it represents a configuration option.
     * 	- The field itself is not modified or altered in any way, as its original state
     * is returned.
     * 
     * @returns a `boolean` value indicating whether the provided `Field` represents a
     * `ConfigOption` type.
     * 
     * The function returns a boolean value indicating whether the given `Field` object
     * represents a `ConfigOption` class.
     * 
     * The `Field` object passed as an argument is checked for type equality with
     * `ConfigOption.class`. If the types match, the function returns `true`, otherwise
     * it returns `false`.
     * 
     * No information about the code author or licensing is provided in the output.
     */
    private static boolean isConfigOption(Field field) {
        return field.getType().equals(ConfigOption.class);
    }

    /**
     * determines whether a `Field` object should be documented based on the absence of
     * certain annotations.
     * 
     * @param field Field object to be evaluated for documentation purposes.
     * 
     * 	- The return value is determined by evaluating two nullability expressions against
     * the field's annotations.
     * 	- `Deprecated.class` and `Documentation.ExcludeFromDocumentation.class` are checked
     * for the presence of annotations on the field. If either is present, the method
     * returns `false`.
     * 	- If both annotations are absent, the method returns `true`.
     * 
     * @returns a boolean value indicating whether the given Field should be documented
     * or not, based on the absence of certain annotations.
     */
    private static boolean shouldBeDocumented(Field field) {
        return field.getAnnotation(Deprecated.class) == null
                && field.getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    /**
     * Transforms this configuration group into HTML formatted table. Options are sorted
     * alphabetically by key.
     *
     * @param options list of options to include in this group
     * @return string containing HTML formatted table
     */
    private static String toHtmlTable(final List<OptionWithMetaInfo> options) {
        StringBuilder htmlTable = new StringBuilder();
        htmlTable.append("<table class=\"configuration table table-bordered\">\n");
        htmlTable.append("    <thead>\n");
        htmlTable.append("        <tr>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 20%\">Key</th>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 15%\">Default</th>\n");
        htmlTable.append("            <th class=\"text-left\" style=\"width: 10%\">Type</th>\n");
        htmlTable.append(
                "            <th class=\"text-left\" style=\"width: 55%\">Description</th>\n");
        htmlTable.append("        </tr>\n");
        htmlTable.append("    </thead>\n");
        htmlTable.append("    <tbody>\n");
        for (OptionWithMetaInfo option : options) {
            htmlTable.append(toHtmlString(option));
        }
        htmlTable.append("    </tbody>\n");
        htmlTable.append("</table>\n");
        return htmlTable.toString();
    }

    /**
     * Transforms option to table row.
     *
     * @param optionWithMetaInfo option to transform
     * @return row with the option description
     */
    private static String toHtmlString(final OptionWithMetaInfo optionWithMetaInfo) {
        return ""
                + "        <tr>\n"
                + "            <td><h5>"
                + escapeCharacters(optionWithMetaInfo.option.key())
                + "</h5></td>\n"
                + "            <td style=\"word-wrap: break-word;\">"
                + escapeCharacters(addWordBreakOpportunities(stringifyDefault(optionWithMetaInfo)))
                + "</td>\n"
                + "            <td>"
                + typeToHtml(optionWithMetaInfo)
                + "</td>\n"
                + "            <td>"
                + getDescription(optionWithMetaInfo)
                + "</td>\n"
                + "        </tr>\n";
    }

    /**
     * generates a summary of an option's description and any additional enum options
     * descriptions, formatted according to a provided formatter.
     * 
     * @param optionWithMetaInfo Option object with additional metadata, providing the
     * option's description and any optional enum options descriptions to be formatted
     * and concatenated for display.
     * 
     * 	- `option`: A reference to an `Option` object, which contains information about
     * a particular option in the application.
     * 	- `metaInfo`: An object that contains metadata related to the option, such as its
     * name, description, and any relevant tags or categories.
     * 	- `formatter`: A formatting object used to format the output of the function.
     * 	- `getEnumOptionsDescription`: A function that retrieves a list of descriptive
     * strings for any enum options associated with the `option`. If no enum options are
     * present, the function returns an empty list.
     * 
     * @returns a string consisting of the option's description followed by any additional
     * enum options descriptions.
     */
    @VisibleForTesting
    static String getDescription(OptionWithMetaInfo optionWithMetaInfo) {
        return formatter.format(optionWithMetaInfo.option.description())
                + Optional.ofNullable(getEnumOptionsDescription(optionWithMetaInfo))
                        .map(formatter::format)
                        .map(desc -> String.format("<br /><br />%s", desc))
                        .orElse("");
    }

    /**
     * Returns a {@link Description} for the enum constants of the given option in case it is
     * enum-based, and {@code null} otherwise.
     */
    private static @Nullable Description getEnumOptionsDescription(
            OptionWithMetaInfo optionWithMetaInfo) {
        Class<?> clazz = getClazz(optionWithMetaInfo.option);
        if (!clazz.isEnum()) {
            return null;
        }
        InlineElement[] optionDescriptions =
                Arrays.stream(clazz.getEnumConstants())
                        .map(ConfigOptionsDocGenerator::formatEnumOption)
                        .map(elements -> TextElement.wrap(elements.toArray(new InlineElement[0])))
                        .toArray(InlineElement[]::new);
        return Description.builder().text("Possible values:").list(optionDescriptions).build();
    }

    /**
     * Formats a single enum constant.
     *
     * <p>If the enum implements {@link DescribedEnum}, this includes the given description for each
     * constant. Otherwise, only the constant itself is printed.
     */
    private static List<InlineElement> formatEnumOption(Object e) {
        final List<InlineElement> elements = new LinkedList<>();
        elements.add(text("\"%s\"", text(escapeCharacters(e.toString()))));

        if (DescribedEnum.class.isAssignableFrom(e.getClass())) {
            elements.add(text(": "));
            elements.add(((DescribedEnum) e).getDescription());
        }

        return elements;
    }

    /**
     * retrieves the Class object associated with a given ConfigOption.
     * 
     * @param option ConfigOption object that contains the configuration value to be
     * retrieved as a Class object.
     * 
     * The method takes an instance of `ConfigOption`, which is defined in the class
     * `ConfigOption`. This class has various attributes and methods that can be used to
     * manipulate and analyze its properties.
     * 
     * One important attribute of `ConfigOption` is its `getClazz` method, which returns
     * a `Class<?>` object representing the class of the option value. This method is
     * declared as a `declaredMethod` in the `ConfigOption` class and can be invoked using
     * reflection.
     * 
     * The `getClazzMethod` instance is created by calling `getDeclaredMethod` on the
     * `ConfigOption` class, followed by setting its accessibility to `true`. The method
     * invocation is performed by passing the `option` argument to the `getClazzMethod`,
     * followed by setting its accessibility back to `false`. Finally, the returned
     * `Class<?>` object is returned from the method.
     * 
     * @returns a `Class` object representing the class of the specified configuration option.
     * 
     * 	- The output is a `Class` object representing the class of the option value.
     * 	- The class is retrieved through the `Method.invoke()` method, which allows for
     * accessing and manipulating the fields and methods of an object.
     * 	- The `setAccessible()` method is used to enable or disable the accessibility of
     * the `Method`, depending on the context in which it is being called.
     */
    private static Class<?> getClazz(ConfigOption<?> option) {
        try {
            Method getClazzMethod = ConfigOption.class.getDeclaredMethod("getClazz");
            getClazzMethod.setAccessible(true);
            Class<?> clazz = (Class<?>) getClazzMethod.invoke(option);
            getClazzMethod.setAccessible(false);
            return clazz;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * takes an `OptionWithMetaInfo` object as input and returns a string representing
     * the type of the option as HTML. Depending on the type of the option, it calls the
     * appropriate helper function to generate the HTML representation.
     * 
     * @param optionWithMetaInfo option and its metadata, which includes the class type
     * of the option.
     * 
     * 	- `option`: This is an instance of `ConfigOption<?>`, which represents an option
     * with metadata in the given configuration.
     * 	- `clazz`: The class of the option type, obtained through reflection.
     * 
     * The function then checks if the option type is an enum, and if so, it calls the
     * `enumTypeToHtml` function to generate the HTML representation of the option.
     * Otherwise, it calls the `atomicTypeToHtml` function to generate the HTML representation
     * of the option's class.
     * 
     * @returns a string representing the HTML type of the given option or class.
     * 
     * 	- If the input `optionWithMetaInfo` is an enum, the function returns the result
     * of calling `enumTypeToHtml()`.
     * 	- Otherwise, the function calls `atomicTypeToHtml()` and passes the class of the
     * option as its argument.
     * 
     * The `typeToHtml` function takes a `ConfigOption<?>` object as input and returns a
     * string representing the type of the option. The output depends on whether the input
     * is an enum or not. If it's an enum, the function calls `enumTypeToHtml()` to
     * generate the HTML representation of the enum values. Otherwise, it calls
     * `atomicTypeToHtml()` to generate the HTML representation of the class of the option.
     */
    @VisibleForTesting
    static String typeToHtml(OptionWithMetaInfo optionWithMetaInfo) {
        ConfigOption<?> option = optionWithMetaInfo.option;
        Class<?> clazz = getClazz(option);
        if (clazz.isEnum()) {
            return enumTypeToHtml();
        }
        return atomicTypeToHtml(clazz);
    }

    /**
     * transforms a given class type into an HTML-escaped string representation.
     * 
     * @param clazz Class object to be converted to an HTML string.
     * 
     * 	- `clazz`: A `Class` object representing the type to convert to HTML.
     * 	- `getSimpleName()`: This method returns a string representation of the class,
     * which is then escaped and returned as the final output.
     * 
     * @returns a HTML-escaped string representation of the given class name.
     */
    private static String atomicTypeToHtml(Class<?> clazz) {
        return escapeCharacters(clazz.getSimpleName());
    }

    /**
     * formats a string representation of an enum type as a HTML paragraph tag, using a
     * specified formatting pattern.
     * 
     * @returns a HTML paragraph containing the text "Enum".
     */
    private static String enumTypeToHtml() {
        return String.format("<p>%s</p>", escapeCharacters("Enum"));
    }

    /**
     * takes an `OptionWithMetaInfo` object and returns the default value of its option
     * field, either by checking for an `Documentation.OverrideDefault` annotation or by
     * calling the option's `defaultValue()` method.
     * 
     * @param optionWithMetaInfo ConfigOption<?> option with additional metadata, including
     * the Documentation.OverrideDefault annotation and the field's default value.
     * 
     * 	- `option`: The ConfigOption object that represents a configuration option in the
     * application.
     * 	- `field`: A reference to the field in the configuration class where the option
     * is defined.
     * 	- `annotation`: An annotation on the field that contains information about default
     * values, documentation, and other metadata.
     * 	- `overrideDocumentedDefault`: An annotation on the field that indicates whether
     * the default value should be overridden based on the documentation. If present, it
     * provides the value to be used as the default.
     * 
     * @returns a string representation of the default value of an option or object.
     * 
     * 	- The first part of the output is the ConfigOption<?> object, which represents
     * the option being processed.
     * 	- The second part of the output is the Documentation.OverrideDefault annotation,
     * which specifies whether the default value of the option has been overridden or
     * not. If the annotation is present, it indicates that the default value has been
     * overridden; otherwise, it is absent.
     * 	- If the overrideDocumentedDefault annotation is absent, the third part of the
     * output is the default value of the option, which can be any valid value type (e.g.,
     * int, boolean, etc.).
     * 	- The final part of the output is the string representation of the default value,
     * which is generated using the `stringifyObject` function. This function takes an
     * object and returns a string representation of it in a standardized format.
     */
    @VisibleForTesting
    static String stringifyDefault(OptionWithMetaInfo optionWithMetaInfo) {
        ConfigOption<?> option = optionWithMetaInfo.option;
        Documentation.OverrideDefault overrideDocumentedDefault =
                optionWithMetaInfo.field.getAnnotation(Documentation.OverrideDefault.class);
        if (overrideDocumentedDefault != null) {
            return overrideDocumentedDefault.value();
        } else {
            Object value = option.defaultValue();
            return stringifyObject(value);
        }
    }

    /**
     * takes an object and returns a string representation of it, depending on its type:
     * string, duration, list or map. If the input is null, it returns "none". Otherwise,
     * it converts the object to a string using appropriate methods.
     * 
     * @param value object to be converted to a string, and the function returns a string
     * representation of that object based on its type.
     * 
     * 	- If `value` is an instance of `String`, then the string value is returned in
     * double quotes. An empty string is returned as "none".
     * 	- If `value` is an instance of `Duration`, then it is formatted using `TimeUtils.formatWithHighestUnit()`.
     * 	- If `value` is an instance of `List`, then each element of the list is deserialized
     * using `stringifyObject`. The elements are joined using the `collect()` method with
     * an empty string delimiter.
     * 	- If `value` is an instance of `Map`, then each key-value pair of the map is
     * deserialized using `stringifyObject`. The keys and values are joined using the
     * `collect()` method with a comma delimiter.
     * 	- If `value` is `null`, then "none" is returned.
     * 
     * @returns a string representation of an object, depending on its type, which includes:
     * 
     * 	- A literal string if the input is a `String`.
     * 	- A formatted duration string if the input is a `Duration`.
     * 	- A list of strings separated by a semicolon if the input is a `List`.
     * 	- A map of keys and values separated by a comma if the input is a `Map`.
     * 	- None if the input is null.
     */
    @SuppressWarnings("unchecked")
    private static String stringifyObject(Object value) {
        if (value instanceof String) {
            if (((String) value).isEmpty()) {
                return "(none)";
            }
            return "\"" + value + "\"";
        } else if (value instanceof Duration) {
            return TimeUtils.formatWithHighestUnit((Duration) value);
        } else if (value instanceof List) {
            return ((List<Object>) value)
                    .stream()
                            .map(ConfigOptionsDocGenerator::stringifyObject)
                            .collect(Collectors.joining(";"));
        } else if (value instanceof Map) {
            return ((Map<String, String>) value)
                    .entrySet().stream()
                            .map(e -> String.format("%s:%s", e.getKey(), e.getValue()))
                            .collect(Collectors.joining(","));
        }
        return value == null ? "(none)" : value.toString();
    }

    /**
     * modifies a given string by replacing semicolons with semicolons followed by a
     * hidden break tag, allowing for easier word breaking in certain situations.
     * 
     * @param value string to be modified, which is replaced with a modified version that
     * allows breaking of semicolon-separated lists using the `<wbr>` tag.
     * 
     * @returns a modified version of the input string that allows for breaking of
     * semicolon-separated lists using HTML breaks.
     */
    private static String addWordBreakOpportunities(String value) {
        return value
                // allow breaking of semicolon separated lists
                .replace(";", ";<wbr>");
    }

    /**
     * sorts a list of `OptionWithMetaInfo` objects based on the value of their `option.key()`
     * attributes using the `Comparator.comparing()` method.
     * 
     * @param configOptions list of options that need to be sorted.
     * 
     * 	- `List<OptionWithMetaInfo>` represents a collection of objects comprising options
     * with metadata.
     * 	- Each `OptionWithMetaInfo` object contains an `option` field and a `metaInfo`
     * field, which provide information about the option and its associated metadata, respectively.
     * 	- The `Comparator` used in the `sort` method is defined as `Comparator.comparing(option
     * -> option.option.key())`, which compares each option based on its key value.
     */
    private static void sortOptions(List<OptionWithMetaInfo> configOptions) {
        configOptions.sort(Comparator.comparing(option -> option.option.key()));
    }

    /**
     * Data structure used to assign {@link ConfigOption ConfigOptions} to the {@link ConfigGroup}
     * with the longest matching prefix.
     */
    private static class Tree {
        private final Node root = new Node();

        Tree(ConfigGroup[] groups, Collection<OptionWithMetaInfo> options) {
            // generate a tree based on all key prefixes
            for (ConfigGroup group : groups) {
                Node currentNode = root;
                for (String keyComponent : group.keyPrefix().split("\\.")) {
                    currentNode = currentNode.addChild(keyComponent);
                }
                currentNode.markAsGroupRoot();
            }

            // assign options to their corresponding group, i.e. the last group root node
            // encountered when traversing
            // the tree based on the option key
            for (OptionWithMetaInfo option : options) {
                findGroupRoot(option.option.key()).assignOption(option);
            }
        }

        /**
         * retrieves a list of configuration options for a given `ConfigGroup`. It first finds
         * the root node of the group using the provided prefix, then returns the list of
         * options associated with that node.
         * 
         * @param configGroup configuration group for which the options are to be found.
         * 
         * 	- `keyPrefix`: The key prefix for which the options are being searched.
         * 	- `Node`: A reference to the root node of a group in the configuration data structure.
         * 
         * @returns a list of `OptionWithMetaInfo` instances representing the configuration
         * options for a given `ConfigGroup`.
         * 
         * 	- `List<OptionWithMetaInfo>` represents a list of configuration options with metadata.
         * 	- `Node` is a class that stores the configuration information and its associated
         * metadata.
         * 	- `groupRoot` is an instance of `Node` representing the root node of the configuration
         * group specified by `configGroup.keyPrefix()`.
         * 	- `getConfigOptions()` returns a list of configuration options associated with
         * the group root node.
         */
        List<OptionWithMetaInfo> findConfigOptions(ConfigGroup configGroup) {
            Node groupRoot = findGroupRoot(configGroup.keyPrefix());
            return groupRoot.getConfigOptions();
        }

        /**
         * returns a list of `OptionWithMetaInfo` objects representing the default configuration
         * options for a Root object.
         * 
         * @returns a list of `OptionWithMetaInfo` objects representing the default configuration
         * options for the system.
         * 
         * 	- The `List<OptionWithMetaInfo>` object represents a list of options with their
         * corresponding metadata.
         * 	- Each option in the list is an `OptionWithMetaInfo` object, which contains the
         * option name and its associated metadata.
         * 	- The metadata includes information about the option's type, description, and any
         * default value it may have.
         */
        List<OptionWithMetaInfo> getDefaultOptions() {
            return root.getConfigOptions();
        }

        /**
         * searches for a group node in a tree-like data structure based on a given key. It
         * recursively traverses the tree, following the key components, and returns the last
         * root node found.
         * 
         * @param key path to the group root node, and it is used to find the corresponding
         * group root node in the tree.
         * 
         * @returns a `Node` object representing the group root.
         * 
         * 	- `lastRootNode`: The last group root node found in the tree, which is also the
         * final destination of the traversal.
         * 	- `currentNode`: The current node being traversed in the tree.
         * 	- `key`: The key used to traverse the tree and find the group root.
         * 	- `root`: The starting point of the traversal, which is typically the root node
         * of the tree.
         */
        private Node findGroupRoot(String key) {
            Node lastRootNode = root;
            Node currentNode = root;
            for (String keyComponent : key.split("\\.")) {
                final Node childNode = currentNode.getChild(keyComponent);
                if (childNode == null) {
                    break;
                } else {
                    currentNode = childNode;
                    if (currentNode.isGroupRoot()) {
                        lastRootNode = currentNode;
                    }
                }
            }
            return lastRootNode;
        }

        /**
         * is used to represent a hierarchical structure of ConfigOptions and their relationships.
         * It has a list of OptionWithMetaInfo objects representing the ConfigOptions and a
         * map of child nodes. The Node class also provides methods for adding and retrieving
         * children, as well as marking a node as a group root.
         */
        private static class Node {
            private final List<OptionWithMetaInfo> configOptions = new ArrayList<>(8);
            private final Map<String, Node> children = new HashMap<>(8);
            private boolean isGroupRoot = false;

            /**
             * adds a new child node to the linked list represented by the `children` map if the
             * key component does not already exist in the map or creates a new node and maps it
             * to the key component if it exists.
             * 
             * @param keyComponent component of a node to which a new child node will be added.
             * 
             * @returns a reference to a newly created `Node` object or the existing `Node` object
             * associated with the given key component.
             * 
             * 	- The function returns a `Node` object representing a child node in the tree structure.
             * 	- If the key component does not exist in the `children` map, a new `Node` object
             * is created and added to the map with the provided key component as its value.
             * 	- The returned `Node` object contains information about the child node, including
             * its key component and any additional attributes or properties that have been
             * assigned to it.
             */
            private Node addChild(String keyComponent) {
                Node child = children.get(keyComponent);
                if (child == null) {
                    child = new Node();
                    children.put(keyComponent, child);
                }
                return child;
            }

            /**
             * retrieves a child node based on its key component, using a hash table to quickly
             * locate the appropriate node.
             * 
             * @param keyComponent component of the node's key that is used to retrieve the
             * corresponding child node from the `children` map.
             * 
             * @returns a `Node` object representing the child element with the specified key component.
             * 
             * The function returns a `Node` object, which represents a child node in a tree-like
             * data structure. The `Node` object contains a reference to the child node itself,
             * as well as additional information about the node such as its key component and the
             * `children` map.
             */
            private Node getChild(String keyComponent) {
                return children.get(keyComponent);
            }

            /**
             * adds an `OptionWithMetaInfo` object to a `List` called `configOptions`.
             * 
             * @param option OptionWithMetaInfo object that is being added to the `configOptions`
             * list.
             * 
             * 	- `configOptions`: A list of `OptionWithMetaInfo` objects that can be manipulated
             * or modified.
             */
            private void assignOption(OptionWithMetaInfo option) {
                configOptions.add(option);
            }

            /**
             * determines whether a given group is a root group or not.
             * 
             * @returns a boolean value indicating whether the current group is a root group or
             * not.
             */
            private boolean isGroupRoot() {
                return isGroupRoot;
            }

            /**
             * sets the object's `isGroupRoot` field to `true`, indicating that it is a group root.
             */
            private void markAsGroupRoot() {
                this.isGroupRoot = true;
            }

            /**
             * retrieves a list of configuration options and returns it.
             * 
             * @returns a list of `OptionWithMetaInfo` objects.
             * 
             * The List<OptionWithMetaInfo> returned is a collection of configuration options
             * with metadata. Each option is represented by an OptionWithMetaInfo object that
             * contains information about the option, such as its name, description, and default
             * value. The List provides a convenient way to access and manipulate the configuration
             * options.
             */
            private List<OptionWithMetaInfo> getConfigOptions() {
                return configOptions;
            }
        }
    }

    /**
     * provides a representation of an option with metadata, including the option and the
     * field associated with it.
     * Fields:
     * 	- option (ConfigOption<?>): in OptionWithMetaInfo represents an option provided
     * by a ConfigOption, along with its corresponding field within a Class object.
     * 	- field (Field): in OptionWithMetaInfo represents an instance of Field from Java.
     */
    static class OptionWithMetaInfo {
        final ConfigOption<?> option;
        final Field field;

        public OptionWithMetaInfo(ConfigOption<?> option, Field field) {
            this.option = option;
            this.field = field;
        }
    }

    private ConfigOptionsDocGenerator() {}
}
