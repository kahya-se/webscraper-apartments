FROM python:3.10.12-slim-bullseye
ENV TZ=Europe/Zurich

RUN pip install --no-cache-dir \ 
                pandas==2.0.3 \ 
                requests \ 
                numpy

WORKDIR /home/work/mobydock

COPY ./scrape_apartment_ads.py /home/work/mobydock/scripts/scrape_apartment_ads.py
COPY ./main.py /home/work/mobydock/scripts/main.py

CMD ["python", "./scripts/main.py"]


