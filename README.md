# My solution

I implemented this using Spark's dataset API. I considered Spark Streaming, but I'm not very familiar with the API.

To run the tests, use

```bash
$ sbt test
```

To run the spark program (using "local" mode), use

```bash
$ sbt run </path/to/logfile>

```

# Results

For the provided log file, my results were the following:

### Average session length (in seconds): 100.73

### Average number of unique URLs/session: 8.31

### Top 10 most engaged users
 
|     ip address    | longest session (s) |
|-------------------|--------------------:|
|     52.74.219.71  |            2069.162 |
|    119.81.61.166  |            2068.849 |
|    106.186.23.95  |            2068.756 |
|     125.19.44.66  |            2068.713 |
|     125.20.39.66  |            2068.320 |
|     192.8.190.10  |            2067.235 |
|    54.251.151.39  |            2067.023 |
|   180.211.69.209  |            2066.961 |
|   180.179.213.70  |            2065.638 |
|   203.189.176.14  |            2065.594 |

# Notes

I have assumed a 15 minute timeout for sessions. I only considered IP addresses, ignoring the port for this analysis.
To distinguish several users on the same IP address (e.g. on a public wifi), one could include the user-agent string as well.
