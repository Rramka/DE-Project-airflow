FROM python:3.9.6

WORKDIR /app

COPY requirements.txt /app

RUN pip install --upgrade pip

RUN pip install -r requirements.txt


ENV PYTHONPATH="${PYTHONPATH}:/app"

COPY . .


ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]

# CMD ["python", "app/main.py"]
