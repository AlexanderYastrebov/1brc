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
./calculate_average_AlexanderYastrebov.sh 20.49
./calculate_average_ebarlas.sh 21.70
./calculate_average_ddimtirov.sh 23.75
./calculate_average_royvanrijn.sh 25.26
./calculate_average_spullara.sh 29.70
./calculate_average_palmr.sh 34.25
./calculate_average_filiphr.sh 45.20
./calculate_average_richardstartin.sh 58.03
./calculate_average_bjhara.sh 64.41
./calculate_average_seijikun.sh 77.34
./calculate_average_truelive.sh 91.86
./calculate_average_criccomini.sh 95.59
./calculate_average_khmarbaise.sh 209.08
./calculate_average_kuduwa-keshavram.sh 241.50
./calculate_average_padreati.sh 245.11
./calculate_average_itaske.sh 251.12
./calculate_average.sh 357.22
```
