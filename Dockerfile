# jupyter image
FROM jupyter/scipy-notebook

# copy files
ADD . /tds
RUN rm -r work

# install requirements
RUN pip install -r /tds/requirements.txt

# add persistent python path (for local imports)
ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/tds"

# jupyter notebook entry
RUN pip install jupyter -U && pip install jupyterlab
EXPOSE 8888
ENTRYPOINT ["jupyter","lab","--ip=0.0.0.0","--allow-root"]
