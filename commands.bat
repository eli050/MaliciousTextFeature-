#run the mongo conteiner
docker run --name mongo -p 27017:27017 -d mongodb/mongodb-community-server:latest

#run the local docker compose for kafka conteiners
docker compose up -d


