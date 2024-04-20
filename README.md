# Description

Deniz Yuksel, M.Sc. Thesis Repository. Ethereum data analysis using Blocknative mempool and archive data. This is a temporary storage so none of the thesis material gets lost.

# Instructions

* In the watchdog_v2.py file, either pick localhost_name or docker_host_name, depending on the setup
* With docker, run docker-compose up --build
* In local development, setup virtualenv, install requirements, and activate the environment
* Start watchdog_v2.py, and then start download_slices.sh with a file parameter (dates.txt or any other file that contains dates in YYYYMMDD format)


# Refined Work Log:

1. I downloaded all the required csv.gz files. To do that, I wrote some bash scripts.
2. Implemented the python script 'watchdog_small.py'
3. I run watchdog_small and let it listen to the filesystem. Then I start the bash scripts with given arguments.
   1. Arguments can be a txt file or the dates by themselves. In the final version, the argument can be the output of dategen.py
4. After watchdog_small creates the db and the table, it writes all the data there. curblocknumber and detect_date have indexes for efficient reading.
   1. Implemented docker scripts for containerization, later on I didn't require those because I realized I can do this project in my own computer.
5. Data write for some time... (transactions table is 11 GB - reduced from 40 GB)
6. Implemented 8 plot scripts, and one final plotgen.py script to generate the plots for the data in transactions table
7. Implemented zeromev.py where I get zeromev data from zeromev API. This script also creates the table 'zeromev_data'
8. Implemented aggregate.py which aggregates data for each block number in transactions, and omits the hashes and some other fields. Creates the table blocknative_blocks
   1. blocknative_blocks - 30MB, zeromev_data - 1359MB.
9. Will implement a new python script that joins these two tables on block_number values (the ones in the blocknative_blocks are more important)
