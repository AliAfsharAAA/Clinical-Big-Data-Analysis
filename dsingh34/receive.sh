ssh dsingh34@jhu.edu@gateway2.marcc.jhu.edu "mv ~/work2/dsingh34/minedsets ~/work2/dsingh34/minedsets_marcc; zip -r ~/work2/dsingh34/transfers.zip ~/work2/dsingh34/minedsets_marcc; mv ~/work2/dsingh34/minedsets_marcc ~/work2/dsingh34/minedsets"
scp dsingh34@jhu.edu@gateway2.marcc.jhu.edu:~/work2/dsingh34/transfers.zip . 
ssh dsingh34@jhu.edu@gateway2.marcc.jhu.edu "rm ~/work2/dsingh34/transfers.zip"

unzip transfers.zip
mv home*/dsingh34@jhu.edu/work2/dsingh34/minedsets_marcc .

rm -r transfers.zip home*

#cp minedsets_marcc/* minedsets
#rm -r minedsets_marcc