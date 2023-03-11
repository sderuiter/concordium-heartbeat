FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

WORKDIR /home/code
RUN cd /home/code

# Install Python dependencies.
COPY ./requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

COPY . .
CMD ["python3", "/home/code/main.py"]