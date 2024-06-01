package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	f, err := os.Open("data/sample.txt")
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var city, stemp string
		splitrecord := strings.Split(scanner.Text(), ";")
		fmt.Sscanf(splitrecord[0], "%s", &city)
		fmt.Sscanf(splitrecord[1], "%s", &stemp)
		itemp := strings.Replace(stemp, ".", "", 1)
		temp, _ := strconv.ParseInt(itemp, 0, 32)

		/*
			next steps:
			 * create a map for the the values using the city as a key
			 * create a slice as the value for each key in the map
			 * append the value for each line into the slice
			 * find min, max, and mean after all lines are read
			 * divide min/max/mean by 10 to replace my mising deicmal
		*/
		fmt.Printf("%s %d\n", city, temp)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}
