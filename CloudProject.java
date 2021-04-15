
import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.ArrayList;
import java.io.File;
import javax.swing.filechooser.*;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CloudProject implements ActionListener{
	
	//hadoop job variables
	//project id
	public static final String projectId = "cloud-computing-1660";
	//project region
	public static final String region = "us-central1";
	//cluster name
	public static final String clusterName = "owens-cluster";
	//hadoop command, the input directory must be 'your bucket name/input' because this program uploads files to your bucket in a input/ folder
	//output directory must be bucket output directory
	public static final String hadoopFsQuery = "InvertedIndexes gs://1660-hw-bucket/input gs://1660-hw-bucket/output";
	//jar file located on cluster (I believe it can be located on the bucket as well, but I only tested on the cluster)
	public static final String jarFile = "gs://1660-hw-bucket/inverted.jar";
	//bucket name
	public static final String bucketName = "1660-hw-bucket";
	//path to your google credentials!
	public static final String jsonPath = "/src/cloudKeys.json";

	//GUI variables!
	public static boolean first_construct = false;
	public static ArrayList<String> selected_files;
	public static JLabel files;
	public static JFrame frame;
	public static JTextField bar;
	public static void main(String[] args){
		//initialize GUI
		initializeGUI();		
	}
	
	//action listener
	public void actionPerformed(ActionEvent e){
		if(e.getActionCommand().equals("1")){
			fileChooser();
		} else if(e.getActionCommand().equals("2") || e.getActionCommand().equals("7")){
			try {
				constructII();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} else if(e.getActionCommand().equals("3")){
			searchScreen();
		} else if(e.getActionCommand().equals("4")){
			topnScreen();
		} else if(e.getActionCommand().equals("5")){
			topnFinal();
		} else if(e.getActionCommand().equals("6")){
			searchFinal();
		}
	}
	
	//search final
	private static void searchFinal(){
		Container pane = frame.getContentPane();
		//get topn-value
		String text = bar.getText();
		
		//search for term
		searchForTerm(text);
		
		//clear screen
		pane.removeAll();

		//new screen
		JButton go_back = new JButton("Go Back to Main Screen: ");
		go_back.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		go_back.addActionListener(new CloudProject());
		go_back.setActionCommand("7");
		
		pane.add(Box.createVerticalGlue());
		pane.add(go_back);
		pane.add(Box.createVerticalGlue());
		pane.revalidate();
		pane.repaint();
	}
	
	//search screen
	private static void searchScreen(){
		Container pane = frame.getContentPane();
		pane.removeAll();
		
		//new screen
		JLabel enter = new JLabel("Enter Your Search Term");
		enter.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JPanel contains_bar = new JPanel();
		contains_bar.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		bar = new JTextField(15);
		bar.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JButton search_topn = new JButton("Search");
		search_topn.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		search_topn.addActionListener(new CloudProject());
		search_topn.setActionCommand("6");
		
		contains_bar.add(bar);
		
		pane.add(Box.createVerticalGlue());
		pane.add(enter);
		pane.add(Box.createVerticalGlue());
		pane.add(contains_bar);
		pane.add(Box.createVerticalGlue());
		pane.add(search_topn);
		pane.add(Box.createVerticalGlue());
		
		pane.revalidate();
		pane.repaint();
	}
	
	//topn results
	private static void topnFinal(){
		Container pane = frame.getContentPane();
		//get topn-value
		String text = bar.getText();
		
		//topn results
		getTopN(text);
		
		//clear screen
		pane.removeAll();

		//new screen
		JButton go_back = new JButton("Go Back to Main Screen");
		go_back.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		go_back.addActionListener(new CloudProject());
		go_back.setActionCommand("7");
		
		pane.add(Box.createVerticalGlue());
		pane.add(go_back);
		pane.add(Box.createVerticalGlue());
		pane.revalidate();
		pane.repaint();
	}
	
	//topn screen
	private static void topnScreen(){
		Container pane = frame.getContentPane();
		pane.removeAll();
		
		//new screen
		JLabel enter = new JLabel("Enter Your N Value");
		enter.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JPanel contains_bar = new JPanel();
		contains_bar.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		bar = new JTextField(15);
		bar.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JButton search_topn = new JButton("Search");
		search_topn.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		search_topn.addActionListener(new CloudProject());
		search_topn.setActionCommand("5");
		
		contains_bar.add(bar);
		
		pane.add(Box.createVerticalGlue());
		pane.add(enter);
		pane.add(Box.createVerticalGlue());
		pane.add(contains_bar);
		pane.add(Box.createVerticalGlue());
		pane.add(search_topn);
		pane.add(Box.createVerticalGlue());
		pane.revalidate();
		pane.repaint();
	}
	
	//construct inverted indices
	private static void constructII() throws IOException, InterruptedException{
		Container pane = frame.getContentPane();
		pane.removeAll();
		
		//construct indices
		if(!first_construct) {
			for(int i=0; i<selected_files.size(); i++) {
				uploadFiles(selected_files.get(i));
			}
			submitHadoopFsJob(projectId, region, clusterName, hadoopFsQuery);
			first_construct = true;
		}
		//new screen
		JLabel success = new JLabel("Engine was loaded and Inverted indices " + 
								"were constructed successfully!");
		success.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JLabel selection = new JLabel("Please Select Action");
		selection.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JButton search_term = new JButton("Search for Term");
		search_term.setAlignmentX(Component.CENTER_ALIGNMENT);
			
		JButton topn = new JButton("Top-N");
		topn.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		search_term.addActionListener(new CloudProject());
		search_term.setActionCommand("3");
		
		topn.addActionListener(new CloudProject());
		topn.setActionCommand("4");
		
		//add all to frame
		pane.add(Box.createVerticalGlue());
		pane.add(success);
		pane.add(Box.createVerticalGlue());
		pane.add(selection);
		pane.add(Box.createVerticalGlue());
		pane.add(search_term);
		pane.add(topn);
		pane.add(Box.createVerticalGlue());
		pane.revalidate();
		pane.repaint();
	}
	
	//choose files gui
	private static void fileChooser(){
		//store selected_files
		selected_files = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();
		
		//working directory for jfilechooser
		File workingDir = new File(System.getProperty("user.dir"));
		
		//set up jfilechooser
		JFileChooser chooser = new JFileChooser(FileSystemView.getFileSystemView()); 
		chooser.setMultiSelectionEnabled(true);
		chooser.setCurrentDirectory(workingDir);
		
		//open jfilechooser
		int selected = chooser.showOpenDialog(null); 
		
		//selected file
		if (selected == JFileChooser.APPROVE_OPTION){ 
			File[] f = chooser.getSelectedFiles();
			for(int i = 0; i < f.length; i++){
				selected_files.add(f[i].getAbsolutePath());
				sb.append(f[i].getAbsolutePath() + "<br>");
			}
			files.setText("<html>" + sb.toString() + "</html>");
		}
	}
	
	//set up gui
	private static void initializeGUI(){
		//create JFrame
		frame = new JFrame("Owen's Search Engine");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frame.setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));
		
		//create home page content
		JLabel load_engine = new JLabel("Load Engine");
		load_engine.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JButton choose_files = new JButton("Choose Files");
		choose_files.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		JButton construct_indices = new JButton("Construct Inverted Indices");
		construct_indices.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		files = new JLabel();
		files.setAlignmentX(Component.CENTER_ALIGNMENT);
		
		//add action listeners
		choose_files.addActionListener(new CloudProject());
		choose_files.setActionCommand("1");
		
		construct_indices.addActionListener(new CloudProject());
		construct_indices.setActionCommand("2");
		
		//add all to frame
		frame.add(Box.createVerticalGlue());
		frame.add(load_engine);
		frame.add(Box.createVerticalGlue());
		frame.add(choose_files);
		frame.add(files);
		frame.add(Box.createVerticalGlue());
		frame.add(construct_indices);
		frame.add(Box.createVerticalGlue());

		//show window
		frame.setSize(800,800);
		frame.setVisible(true);
	}
	
	//topn algorithm
	private static void getTopN(String text) {
		//get topn
	}
	
	//search for term
	private static void searchForTerm(String to_find) {
		/*String destFilePath = "MRoutput.txt";
	    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

	    Blob blob = storage.get(BlobId.of(bucketName, "output"));
	    blob.downloadTo(Paths.get(destFilePath));*/

	}
	
	//helper method for submitting hadoop job
	public static ArrayList<String> stringToList(String s) {
		return new ArrayList<>(Arrays.asList(s.split(" ")));
	}

	//upload files selected to construct indices
	public static void uploadFiles(String filePath) throws IOException {			
			String objectName = "";
			//get file name
			for(int i=filePath.length()-1; i>=0; i--) {
				if(filePath.charAt(i) == '\\') {
					objectName = filePath.substring(i+1, filePath.length());
					break;
				}
			}

		    // You can specify a credential file by providing a path to GoogleCredentials.
		    // Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
		    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
		          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		 
			Storage storage = StorageOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build().getService();
			BlobId blobId = BlobId.of(bucketName, "input/" + objectName);
			BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/x-tar").build();
			storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));
			
			System.out.println("File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
	}
	
	//submit a hadoop job
	public static void submitHadoopFsJob(String projectId, String region, String clusterName, String hadoopFsQuery) throws IOException, InterruptedException {
		String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
		String jsonPath = "cloudKeys.json";
		// You can specify a credential file by providing a path to GoogleCredentials.
		// Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
		GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
		      .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		
		// Configure the settings for the job controller client.
		JobControllerSettings jobControllerSettings =
		    JobControllerSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(credentials)).setEndpoint(myEndpoint).build();
		
		// Create a job controller client with the configured settings. Using a try-with-resources
		// closes the client,
		// but this can also be done manually with the .close() method.
		try (JobControllerClient jobControllerClient =
		    JobControllerClient.create(jobControllerSettings)) {
		
			// Configure cluster placement for the job.
			JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
		
			// Configure Hadoop job settings. The HadoopFS query is set here.
			HadoopJob hadoopJob =
					HadoopJob.newBuilder()
					.setMainJarFileUri(jarFile)
					.addAllArgs(stringToList(hadoopFsQuery))
					.build();
		
			Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();
		
			// Submit an asynchronous request to execute the job.
			OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
					jobControllerClient.submitJobAsOperationAsync(projectId, region, job);
		
			Job response = submitJobAsOperationAsyncRequest.get();
		
			// Print output from Google Cloud Storage.
			Matcher matches =
					Pattern.compile("gs://(.*?)/(.*)").matcher(response.getDriverOutputResourceUri());
					matches.matches();
		
			Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
			Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));
		
			System.out.println(String.format("Job finished successfully: %s", new String(blob.getContent())));
		
		} catch (ExecutionException e) {
			// If the job does not complete successfully, print the error message.
			System.err.println(String.format("submitHadoopFSJob: %s ", e.getMessage()));
		}
	}
}