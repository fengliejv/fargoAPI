sudo docker build -t api_v2.0_pando .
sudo docker stop api_v2.0_pando
sudo docker remove api_v2.0_pando
sudo docker run --restart=always --add-host=host.docker.internal:host-gateway -v /home/pando:/home -d --name api_v2.0_pando -p 9535:9535 api_v2.0_pando