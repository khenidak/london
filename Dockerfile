ARG BASEIMAGE="us.gcr.io/k8s-artifacts-prod/build-image/debian-base-amd64:buster-v1.4.0"
FROM $BASEIMAGE

COPY ./_output/london /bin/

ENTRYPOINT [ "/bin/london" ]
