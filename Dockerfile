FROM python:3.8-slim
WORKDIR /api
COPY . /api
RUN pip3 install -r requirements.txt
RUN pip3 install PyJWT
EXPOSE 9528 9536
CMD ["python", "/api/api/v1/app.py"]