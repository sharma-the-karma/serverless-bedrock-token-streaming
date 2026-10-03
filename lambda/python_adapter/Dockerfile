# Production Dockerfile for Python AWS Lambda with Web Adapter Response Streaming
FROM public.ecr.aws/awslabs/aws-lambda-web-adapter:0.8.4 AS aws-lwa
FROM public.ecr.aws/docker/library/python:3.11-slim

# Copy AWS Lambda Web Adapter binary to /opt/extensions/
COPY --from=aws-lwa /lambda-adapter /opt/extensions/lambda-adapter

# Crucial configuration to enable response streaming mode in Web Adapter
ENV PORT=8080
ENV AWS_LWA_INVOKE_MODE=response_stream
ENV AWS_LWA_ASYNC_INIT=true

WORKDIR /var/task

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
