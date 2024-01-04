# 1brc in go

```sh
$ java -version
java version "21.0.1" 2023-10-17
Java(TM) SE Runtime Environment Oracle GraalVM 21.0.1+12.1 (build 21.0.1+12-jvmci-23.1-b19)
Java HotSpot(TM) 64-Bit Server VM Oracle GraalVM 21.0.1+12.1 (build 21.0.1+12-jvmci-23.1-b19, mixed mode, sharing)

$ ./mvnw clean verify
...
[INFO] BUILD SUCCESS
...

$ ln -s measurements-109.txt measurements.txt
$ wc -l measurements.txt
1000000000 measurements.txt

# show elapsed time in seconds
$ export TIME="%e"

$ for c in ./calculate_average*.sh; do (echo -n "$c " && $c >/dev/null) 2>&1 ; done | sort -k2 -n
./calculate_average_spullara.sh 14.70
./calculate_average_ebarlas.sh 15.74
./calculate_average_filiphr.sh 16.51
./calculate_average_royvanrijn.sh 17.13
./calculate_average_ddimtirov.sh 17.41
./calculate_average_AlexanderYastrebov.sh 17.75
./calculate_average_artsiomkorzun.sh 19.53
./calculate_average_richardstartin.sh 23.41
./calculate_average_lawrey.sh 23.80
./calculate_average_palmr.sh 25.76
./calculate_average_bjhara.sh 50.64
./calculate_average_seijikun.sh 53.32
./calculate_average_truelive.sh 66.54
./calculate_average_criccomini.sh 71.00
./calculate_average_khmarbaise.sh 167.63
./calculate_average_kuduwa-keshavram.sh 174.02
./calculate_average_padreati.sh 186.50
./calculate_average_itaske.sh 190.41
./calculate_average_baseline.sh 262.48
```
