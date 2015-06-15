# How to use Sedna in your programs.

# Introduction #

Briefly speaking, the usage of Sedna in your programs is quite easy. Add Sedna.jar into your lib directory, then:
```
import org.mcl.Sedna.Client.FileSystem;
import org.mcl.Sedna.Configuration.Configuration;
```

After importing the necessary class, users of Sedna can read or write data like following code.
```
Configuration conf = new Configuration();
FileSystem fs = new FileSystem(conf);
String key = "test";
String value = "test value";
fs.write(key, value);
...
...
String v = fs.read_latest(key);
```

# Read and Write #

In last section.

# Real time Process #

...to be continue...