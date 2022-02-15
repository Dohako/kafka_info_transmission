# kafka info transmission

## Structure

It worth noticing that here I assume that both Consumer and Producer is a single git repo to make cicd.

If they are in one repo -> update one = update other

If there is no git repo -> nothing happens

* Consumer

  * tests

  * cicd

  * kafka consumer

  * psql filler

* Producer

  * tests

  * cicd

  * kafka producer

## How to run

if docker: docker-compose up -d --b

if other - run main in each data handler (consumer and producer)

## Current todo

* ~~create Consumer~~

* ~~create Producer~~

* ~~create dockers~~

* ~~connect to Aiven~~

* ~~create tests for Consumer~~

* ~~create tests for Producer~~

* ~~develop cicd for Consumer~~

* ~~develop cicd for Producer~~
