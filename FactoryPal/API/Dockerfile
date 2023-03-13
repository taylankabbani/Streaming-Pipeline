FROM python:3.9

# dump logs to 1
ENV PYTHONUNBUFFERED 1

# create project structure
RUN mkdir /API
WORKDIR /API

# copy api project files
COPY . .

# install dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# expose the port
EXPOSE 5000

CMD ["python", "src/app.py"]