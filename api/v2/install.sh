sudo docker build -t api_v2.0 .
sudo docker stop api_v2.0
sudo docker remove api_v2.0
sudo docker run --restart=always --add-host=host.docker.internal:host-gateway -v /home:/home -d --name api_v2.0 -p 9530:9530 api_v2.0