ARG TESTDATA_PRODUCER_TAG
FROM $TESTDATA_PRODUCER_TAG as mbei-testdata-producer

FROM python:3.10.2-buster as runtime
WORKDIR /scripts
COPY . .
COPY --from=mbei-testdata-producer /usr/local/bin/mbei-testdata-writer /usr/local/bin
COPY --from=mbei-testdata-producer /usr/local/bin/mbei-testdata-file-producer /usr/local/bin

WORKDIR /scripts
