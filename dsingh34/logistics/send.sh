zip transfers.zip *.py
scp transfers.zip dsingh34@jhu.edu@gateway2.marcc.jhu.edu:~/work2/dsingh34
ssh dsingh34@jhu.edu@gateway2.marcc.jhu.edu "unzip -o ~/work2/dsingh34/transfers.zip -d ~/work2/dsingh34; rm ~/work2/dsingh34/transfers.zip"
rm transfers.zip
