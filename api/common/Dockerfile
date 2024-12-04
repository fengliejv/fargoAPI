FROM python:3.8-slim
ADD .. /code
WORKDIR /code
RUN pip install -r requirements.txt
EXPOSE 9527
CMD ["python", "/code/app.py"]