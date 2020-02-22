#+-+-+-+-+-+-+-+-+-+-+
#|p|y|t|h|o|n|-|e|n|v|
#+-+-+-+-+-+-+-+-+-+-+
FROM python:3.7.6-slim-buster AS compile-image
RUN apt-get update
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# Install dependencies
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

FROM python:3.7.6-slim-buster AS build-image
COPY --from=compile-image /opt/venv /opt/venv
COPY * /usr/src/
#
WORKDIR /usr/src/
# Make sure we use the virtualenv
ENV PATH="/opt/venv/bin:$PATH"
EXPOSE 5000
CMD ["flask", "run", "--host", "0.0.0.0"]
#ENTRYPOINT ["python","app.py"]
#RUN bash entry.sh
