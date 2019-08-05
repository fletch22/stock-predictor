
SET TAG=sp-redis
SET FILE=Dockerfile-redis
SET NAME=%TAG%-name

docker stop %NAME%

docker rm %NAME%

docker build -t %TAG% -f %FILE% .

docker run -it --name %NAME% -p 6379:6379 %TAG%