package co.za.absa.main;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @author DP
 *
 */
@Slf4j
public class MergeFiles {

	private static final String IP_PATTERN = "^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$";
	private static final String VALUE_PATTERN = "\\d+(,\\d+)*$";
	private static final String FILTER = ": ";

	/**
	 * 
	 * @param args
	 */
	public static void main(String args[]) {
		try {
			log.info("File processing start.");
			if (args.length != 2) {
				log.error("Error in number of arguments.");
				return;
			}

			if (StringUtils.isEmpty(args[0]) || StringUtils.isEmpty(args[1])) {
				log.error("One of the argument is NULL.");
			}

			List<FileContent> file1Content = readFileContent(args[0],FILTER);
			List<FileContent> file2Content = readFileContent(args[1],FILTER);
			log.debug("File content read complete.");

			// Validate file content (each line)
			Map<String, Set<String>> file1ContentMap = validateAndPrepareMap(file1Content);
			Map<String, Set<String>> file2ContentMap = validateAndPrepareMap(file2Content);
			log.debug("ValidateAndPrepareMap complete.");
			
			System.out.println("The final output of the file as follows::\n\n");
			// Merge File content
			Stream.of(file1ContentMap, file2ContentMap).flatMap(map -> map.entrySet().stream()).collect(
					Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> new HashSet<String>() {
						{
							addAll(v1);
							addAll(v2);
						}
					})).forEach((key, value) -> System.out
							.println(key + FILTER + value.stream().collect(Collectors.joining(","))));

		} catch (IOException ioe) {
			log.error("An error while fetching file content and error message is {}", ioe.getMessage());
		} catch (RuntimeException re) {
			log.error("There is an error while validating file content and error message is:: {}", re.getMessage());
		} catch (Exception e) {
			log.error("An error occured while processing. Error message is {}", e.getMessage());
		}
		log.info("File processing end.");
	}

	private static Map<String, Set<String>> validateAndPrepareMap(List<FileContent> fileContent) {
		log.info("validateAndPrepareMap.");
		return fileContent.stream().collect(Collectors.toMap(FileContent::getIp, FileContent::getValue));
	}

	/**
	 * 
	 * @param input
	 * @return
	 */
	private static FileContent validateEachLine(String input) {
		log.info("validateEachLine start.");
		String[] value = input.split(FILTER);
		if (value == null || value.length != 2) {
			log.error("After split of input({}) value not valid values.", input);
		}
		if (StringUtils.isEmpty(value[0]) || !value[0].matches(IP_PATTERN)) {
			log.error("{} value having issue with IP_PATTERN of input value {}", value[0], input);
			throw new RuntimeException(value[0] + " Invalid IP");
		}

		if (StringUtils.isEmpty(value[1]) || !value[1].matches(VALUE_PATTERN)) {
			log.error("{} value having issue with VALUE_PATTERN of input value {}", value[1], input);
			throw new RuntimeException(value[1] + " Invalid VALUE");
		}
		log.info("validateEachLine end.");
		return new FileContent(value[0], Stream.of(value[1].split(",", -1)).collect(Collectors.toSet()));
	}

	/**
	 * 
	 * @param fileName
	 * @param filter
	 * @return
	 * @throws IOException
	 */
	private static List<FileContent> readFileContent(String fileName, String filter) throws IOException {
		log.info("readFileContent start.");
		List<FileContent> fileContentList = new ArrayList<FileContent>();
		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
			stream.filter(line -> line.contains(filter)).forEach(value -> {
				fileContentList.add(validateEachLine(value));
			});
		}
		log.info("readFileContent start.");
		return fileContentList;
	}
}

/**
 * 
 * @author DP
 *
 */
class FileContent {
	private String ip;
	private Set<String> value;

	public FileContent(String ip, Set<String> value) {
		this.ip = ip;
		this.value = value;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public Set<String> getValue() {
		return value;
	}

	public void setValue(Set<String> value) {
		this.value = value;
	}
}