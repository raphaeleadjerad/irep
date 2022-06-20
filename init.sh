curl -sL https://deb.nodesource.com/setup_14.x | sudo -E bash -

sudo apt-get update
sudo apt-get upgrade

sudo apt-get install -y nodejs

sudo apt-get install npm
npm install elasticdump

elasticdump \
  --input=http://elasticsearch-master.projet-ssplab:9200/sirus_2020 \
  --output=http://elasticsearch-master:9200/sirus_2020 \
  --type=analyzer
elasticdump \
  --input=http://elasticsearch-master.projet-ssplab:9200/sirus_2020 \
  --output=http://elasticsearch-master:9200/sirus_2020 \
  --type=mapping
elasticdump \
  --input=http://elasticsearch-master.projet-ssplab:9200/sirus_2020 \
  --output=http://elasticsearch-master:9200/sirus_2020 \
  --type=data