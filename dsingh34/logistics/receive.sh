ssh dsingh34@jhu.edu@gateway2.marcc.jhu.edu "zip -r ~/work2/dsingh34/transfers.zip ~/work2/dsingh34/processed_data"
scp dsingh34@jhu.edu@gateway2.marcc.jhu.edu:~/work2/dsingh34/transfers.zip ~/Desktop/jhmi 
ssh dsingh34@jhu.edu@gateway2.marcc.jhu.edu "rm ~/work2/dsingh34/transfers.zip"
unzip transfers.zip
mv home*/dsingh34@jhu.edu/work2/dsingh34/processed_data .
rm -r transfers.zip home*
