package au.edu.utas.lm_nfs_sg.saas.worker.job;

import au.edu.utas.lm_nfs_sg.saas.comms.SocketCommunication;

class KFrequentCount {
	private int num_most_frequent;
	private String[] id_array;
	private int[] count_array;
	private volatile Boolean running = true;

	KFrequentCount(int n) {
		num_most_frequent = n;
		id_array = new String[n];
		count_array = new int[n];

		for(int i = 0; i<num_most_frequent; i++){
			count_array[i] = 0;
			id_array[i] = "";
		}
	}

	void frequentAlgorithm(String s) {
		if (running) {
			boolean string_in_array = false;
			int current_id;

			// -1 = no empty slot
			int empty_id_index = -1;

			// Loop through elements and increment counter is string already in array
			for (current_id = 0; current_id < num_most_frequent; current_id++) {
				// If string in array - increment counter - and then exit loop
				if (id_array[current_id].equalsIgnoreCase(s)) {
					count_array[current_id]++;
					string_in_array = true;
					break;
				}
				// If not - find if there is an empty slot to store new string
				else {
					if (id_array[current_id].equals("")) {
						empty_id_index = current_id;
					}
				}

			}

			// If the string doesn't exist in the array
			if (!string_in_array) {
				// if there is an empty slot - add string and set counter to 1
				if (empty_id_index != -1) {
					id_array[empty_id_index] = s;
					count_array[empty_id_index] = 1;
				}
				// else - there are no empty slots + the input string doesn't exist => decrement all counters
				else {
					for (current_id = 0; current_id < num_most_frequent; current_id++) {
						count_array[current_id]--;

						// if counter is less than 1 - delete item
						if (count_array[current_id] <= 0) {
							id_array[current_id] = "";
							count_array[current_id] = 0;
						}
					}
				}
			}
		}
		// else - drop word
	}

	String sendResults(SocketCommunication sc) {
		running = false;
		String returnStr = "";
		returnStr += "Count\tID\n";
		for (int current_id = 0; current_id < num_most_frequent; current_id++) {
			String newPart = count_array[current_id]+"\t"+id_array[current_id]+"\n";
			if ((returnStr + newPart).getBytes().length > 8192) {
				System.out.println("send results");
				sc.sendMessage(returnStr);
				returnStr = "";
			} else {
				returnStr += newPart;
			}

		}
		sc.sendMessage(returnStr);
		sc.sendMessage("results_finished");
		running = true;
		return returnStr;
	}

}
