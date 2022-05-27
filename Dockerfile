FROM htcondor/submit:9.2-el7

RUN curl -o /etc/yum.repos.d/pegasus.repo http://download.pegasus.isi.edu/wms/download/rhel/7/pegasus.repo \
    && yum -y install pegasus