FROM python:3.10-slim
WORKDIR /api
COPY . /api
RUN pip3 install -r requirements.txt
RUN pip3 install PyJWT
RUN pip3 install cryptography
EXPOSE 9530
CMD ["python", "/api/app.py"]
