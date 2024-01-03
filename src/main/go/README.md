# 1brc in go

```sh
$ java -version
java version "21.0.1" 2023-10-17 LTS
Java(TM) SE Runtime Environment (build 21.0.1+12-LTS-29)
Java HotSpot(TM) 64-Bit Server VM (build 21.0.1+12-LTS-29, mixed mode, sharing)

$ ./mvnw clean verify
...
[INFO] BUILD SUCCESS
...

$ ln -s measurements-109.txt measurements.txt
$ wc -l measurements.txt
1000000000 measurements.txt

# show elapsed time in seconds
$ export TIME="%e"

$ for c in ./calculate_average*.sh; do (echo -n "$c " && $c >/dev/null) 2>&1 ; done | sort -k2 -g
./calculate_average_AlexanderYastrebov.sh 19.45
./calculate_average_ebarlas.sh 20.50
./calculate_average_spullara.sh 24.70
./calculate_average_palmr.sh 25.11
./calculate_average_seijikun.sh 59.96
./calculate_average_bjhara.sh 65.13
./calculate_average_criccomini.sh 74.26
./calculate_average_truelive.sh 85.95
./calculate_average_padreati.sh 89.81
./calculate_average_khmarbaise.sh 152.54
./calculate_average_itaske.sh 172.98
./calculate_average_kuduwa-keshavram.sh 173.64
./calculate_average_royvanrijn.sh 178.19
./calculate_average.sh 311.24
```
