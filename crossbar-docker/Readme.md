# Crossbar on Docker

## 1 - Requirements for local dev on OSX

- Install [Docker toolbox](https://www.docker.com/docker-toolbox)
- Once the installation is complete, 
	- open the **Docker Quickstart Terminal**, this should be installed in the **Docker** directory within **Applications** on your mac. ![screenshot](https://s3-eu-west-1.amazonaws.com/uploads-eu.hipchat.com/30565/816445/GfzEUkIpd0FOlyU/Screen%20Shot%202015-12-14%20at%2013.29.02.png)

	- Alternatively you can do it via the command line directly by running the following command from Terminal `open "/Applications/Docker/Docker Quickstart Terminal.app"` 

- Either of the above approaches should open a new terminal displaying the docker logo. 

>
>
>                        ##         .
>                  ## ## ##        ==
>               ## ## ## ## ##    ===
>           /"""""""""""""""""\___/ ===
>      ~~~ {~~ ~~~~ ~~~ ~~~~ ~~~ ~ /  ===- ~~~
>           \______ o           __/
>             \    \         __/
>              \____\_______/
>
>
>docker is configured to use the default machine with IP 192.168.99.100
>For help getting started, check out the docs at https://docs.docker.com
>
>ofss-MacBook-Pro:docker edu$ 

**If you see this** you can proceed to the next step.

## 2 - Building and Running Docker the container that will host crossbar

- Open a new terminal window and navigate to the same directory as this readme file. 
- from here, make the BASH files executable using `chmod u+x *.sh` and then:
	- To build and run the docker image execute `./first_setup_and_run.sh`
	- and to jut run it do `./start_post_setup.sh`
- Either of these should terminate by displaying the list of docker containers running as below:

>CONTAINER ID        IMAGE               COMMAND                  CREATED             STATUS              PORTS                    NAMES
>8fa5006a2a05        crossbar            "crossbar start --cbd"   45 seconds ago      Up 43 seconds       0.0.0.0:8080->8080/tcp   tender_dubinsky

**If you see that** in the terminal it means that all is running well.

## 3 - Communicating with crossbar from outside the docker container

For this step you will need to get into this terminal of the linux virtual machine that docker is running on.

- To do so you can double click on the virtual machine's icon within Virtual Box. Note: This is only possible with versions of Virtual Box 5 and above.
![](https://s3-eu-west-1.amazonaws.com/uploads-eu.hipchat.com/30565/816445/z4dxpz68QnovhLc/Screen%20Shot%202015-12-14%20at%2013.49.33.png)

- Once inside execute `ifconfig | more` within that terminal
![](https://s3-eu-west-1.amazonaws.com/uploads-eu.hipchat.com/30565/816445/5PLXrE1VZURtwhq/Screen%20Shot%202015-12-14%20at%2015.26.42.png)

- then navigate down with the space bar until you see the `eth1` paragraph
![](https://s3-eu-west-1.amazonaws.com/uploads-eu.hipchat.com/30565/816445/g32USeIRw7OMsKk/Screen%20Shot%202015-12-14%20at%2015.27.00.png)

- In this case the IP that we are interested in is `192.168.99.100`.

Take note of this IP address and proceed to the next step.

## 4 - Testing the URI from your browser

- Open an **incognito window** on you favourite browser and 
	- this will prevent false positives as incognito windows do not use caching.
- go to the relevant IP with `:8080` added at the end to point at the correct port, e.g. [http://192.168.99.100:8080/](http://192.168.99.100:8080/).


If all is working well, you should see something similar to the screenshot below.

- This may seem contradictory as it displays a 404 error page but [the documentation](http://crossbar.io/docs/The-Command-Line/) says this means that all is working well.

>Open http://localhost:8080 in your browser. You should see a 404 page rendered by Crossbar.io. Which means: it works! 

![](https://s3-eu-west-1.amazonaws.com/uploads-eu.hipchat.com/30565/816445/RBHeWUbYxEs8YGR/Screen%20Shot%202015-12-14%20at%2016.08.16.png)

