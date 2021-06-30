# Flink框架学习总结

## 1. Flink介绍

### 1.1 概览

#### 1.1.1 什么是Flink？

> Apache Flink is a framework and distributed processing engine for stateful computations over *unbounded and bounded* data streams. Flink has been designed to run in *all common cluster environments*, perform computations at *in-memory speed* and at *any scale*.

**Apache Flink**是一个框架和分布式的处理引擎，用于对有界和无界的数据流进行有状态计算。Flink被设计为可以在所有常见的集群环境中运行，以内存速度和任意规模去执行计算。

##### 有界和无界数据

任何类型的数据都是以事件流的形式产生。例如信用卡交易、传感器数据、网页或者手机上用户交互行为...，所有的数据都是以流的形式生成。

根据对数据处理的方式可以把数据分为**有界流**和**无界流**

1. **无界流**有一个起点，但没有定义的终点。它们不会终止并在生成数据时提供数据。无界流必须被连续处理，即，事件在被摄取后必须被及时处理。无法等待所有输入数据到达，因为输入是无界的，并且在任何时间都不会完成。处理无边界数据通常需要以特定顺序（例如事件发生的顺序）来摄取事件，以便能够推断出结果的完整性。
2. **有界流**具有定义的开始和结束。可以通过在执行任何计算之前提取所有数据来处理有界流。由于有界数据集始终可以排序，因此不需要有序摄取即可处理有界流。绑定流的处理也称为批处理。

![image-20201209165336254](D:\Markdown\images\Flink\image-20201209165336254.png)

**Apache Flink擅长处理无边界和有边界的数据集。**对时间和状态的精确控制使Flink的运行时能够在无限制的流上运行任何类型的应用程序。有界流由专门为固定大小的数据集设计的算法和数据结构在内部进行处理，从而产生出色的性能。

##### 程序可以部署在任何地方

Apache Flink是一个分布式系统，需要计算资源才能执行应用程序。Flink与所有常见的集群资源管理器（如[Hadoop YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)，[Apache Mesos](https://mesos.apache.org/)和[Kubernetes）集成，](https://kubernetes.io/)但也可以设置为作为独立集群运行。

Flink旨在与前面列出的每个资源管理器配合使用。这是通过特定于资源管理器的部署模式实现的，该模式允许Flink以惯用方式与每个资源管理器进行交互。

部署Flink应用程序时，Flink会根据应用程序配置的并行性自动识别所需的资源，并向资源管理器请求它们。如果发生故障，Flink会通过请求新资源来替换发生故障的容器。提交或控制应用程序的所有通信均通过REST调用进行。这简化了Flink在许多环境中的集成

##### 任意规模运行应用程序

Flink旨在运行任何规模的有状态流应用程序。应用程序被并行化为可能成千上万的任务，这些任务在集群中分布并同时执行。因此，应用程序几乎可以利用无限数量的CPU，主内存，磁盘和网络IO。而且，Flink易于维护非常大的应用程序状态。它的异步和增量检查点算法可确保对处理延迟的影响最小，同时可保证一次状态一致性。

##### 利用内存性能

有状态Flink应用程序针对本地状态访问进行了优化。任务状态始终保持在内存中，或者，如果状态大小超出可用内存，则始终保持在访问有效的磁盘数据结构中。因此，任务通过访问通常在内存中的本地状态执行所有计算，从而产生非常低的处理延迟。Flink通过定期将本地状态异步指向持久性存储来确保出现故障时的一次状态一致性。

![image-20201209165653743](D:\Markdown\images\Flink\image-20201209165653743.png)

#### 1.1.2 Flink数据处理

##### 流式处理Dataflows

在自然环境中，数据的产生原本就是流式的。无论是来自 Web 服务器的事件数据，证券交易所的交易数据，还是来自工厂车间机器上的传感器数据，其数据都是流式的。但是当你分析数据时，可以围绕 *有界流*（*bounded*）或 *无界流*（*unbounded*）两种模型来组织处理数据，当然，选择不同的模型，程序的执行和处理方式也都会不同。

- **批处理**是有界数据流处理的范例。在这种模式下，你可以选择在计算结果输出之前输入整个数据集，这也就意味着你可以对整个数据集的数据进行排序、统计或汇总计算后再输出结果。
- **流处理**正相反，其涉及无界数据流。至少理论上来说，它的数据输入永远不会结束，因此程序必须持续不断地对到达的数据进行处理。

在 Flink 中，应用程序由用户自定义**算子**转换而来的**流式 dataflows** 所组成。这些流式 dataflows 形成了有向图，以一个或多个**数据源**（source）开始，并以一个或多个**汇**（sink）结束。

![image-20201209170013135](D:\Markdown\images\Flink\image-20201209170013135.png)

Flink 应用程序可以消费来自消息队列或分布式日志这类流式数据源（例如 **Apache Kafka** 或 **Kinesis**）的实时数据，也可以从各种的数据源中消费有界的历史数据。同样，Flink 应用程序生成的结果流也可以发送到各种数据汇中。

![image-20201209170138514](D:\Markdown\images\Flink\image-20201209170138514.png)

##### 并行Dataflows

Flink 程序本质上是分布式并行程序。在程序执行期间，一个流有一个或多个**流分区**（Stream Partition），每个算子有一个或多个**算子子任务**（Operator Subtask）。每个子任务彼此独立，并在不同的线程中运行，或在不同的计算机或容器中运行

==**算子子任务数就是其对应算子的并行度。在同一程序中，不同算子也可能具有不同的并行度。**==

![image-20201209170432706](D:\Markdown\images\Flink\image-20201209170432706.png)

**Flink 算子之间可以通过*一对一*（*直传*）模式或*重新分发*模式传输数据：**

- **一对一**模式（例如上图中的 *Source* 和 *map()* 算子之间）可以保留元素的分区和顺序信息。这意味着 *map()* 算子的 subtask[1] 输入的数据以及其顺序与 *Source* 算子的 subtask[1] 输出的数据和顺序完全相同，即同一分区的数据只会进入到下游算子的同一分区。
- **重新分发**模式（例如上图中的 *map()* 和 *keyBy/window* 之间，以及 *keyBy/window* 和 *Sink* 之间）则会更改数据所在的流分区。当你在程序中选择使用不同的 *transformation*，每个*算子子任务*也会根据不同的 transformation 将数据发送到不同的目标子任务。例如以下这几种 transformation 和其对应分发数据的模式：*keyBy()*（通过散列键重新分区）、*broadcast()*（广播）或 *rebalance()*（随机重新分发）。在*重新分发*数据的过程中，元素只有在每对输出和输入子任务之间才能保留其之间的顺序信息（例如，*keyBy/window* 的 subtask[2] 接收到的 *map()* 的 subtask[1] 中的元素都是有序的）。因此，上图所示的 *keyBy/window* 和 *Sink* 算子之间数据的重新分发时，不同键（key）的聚合结果到达 Sink 的顺序是不确定的。

##### 自定义事件流处理

对于大多数流数据处理应用程序而言，能够使用处理实时数据的代码重新处理历史数据并产生确定并一致的结果非常有价值。

在处理流式数据时，我们通常更需要关注事件本身发生的顺序而不是事件被传输以及处理的顺序，因为这能够帮助我们推理出一组事件（事件集合）是何时发生以及结束的。例如电子商务交易或金融交易中涉及到的事件集合。

为了满足上述这类的实时流处理场景，我们通常会使用记录在数据流中的事件时间的时间戳，而不是处理数据的机器时钟的时间戳。

##### 有状态流处理

==**Flink 中的算子可以是有状态的。**==这意味着如何处理一个事件可能取决于该事件之前所有事件数据的累积结果。Flink 中的状态不仅可以用于简单的场景（例如统计仪表板上每分钟显示的数据），也可以用于复杂的场景（例如训练作弊检测模型）。

Flink 应用程序可以在分布式群集上并行运行，其中每个算子的各个并行实例会在单独的线程中独立运行，并且通常情况下是会在不同的机器上运行。

有状态算子的并行实例组在存储其对应状态时通常是按照键（key）进行分片存储的。每个并行实例算子负责处理一组特定键的事件数据，并且这组键对应的状态会保存在本地。

如下图的 Flink 作业，其前三个算子的并行度为 2，最后一个 sink 算子的并行度为 1，其中第三个算子是有状态的，并且你可以看到第二个算子和第三个算子之间是全互联的（fully-connected），它们之间通过网络进行数据分发。通常情况下，实现这种类型的 Flink 程序是为了通过某些键对数据流进行分区，以便将需要一起处理的事件进行汇合，然后做统一计算处理。

![image-20201209170635049](D:\Markdown\images\Flink\image-20201209170635049.png)

Flink 应用程序的状态访问都在本地进行，因为这有助于其提高吞吐量和降低延迟。通常情况下 Flink 应用程序都是将状态存储在 JVM 堆上，但如果状态太大，我们也可以选择将其以结构化数据格式存储在高速磁盘中。

##### 通过状态快照实现的容错

通过状态快照和流重放两种方式的组合，Flink 能够提供可容错的，精确一次计算的语义。这些状态快照在执行时会获取并存储分布式 pipeline 中整体的状态，它会将数据源中消费数据的偏移量记录下来，并将整个 job graph 中算子获取到该数据（记录的偏移量对应的数据）时的状态记录并存储下来。当发生故障时，Flink 作业会恢复上次存储的状态，重置数据源从状态中记录的上次消费的偏移量开始重新进行消费处理。而且状态快照在执行时会异步获取状态并存储，并不会阻塞正在进行的数据处理逻辑。

<div STYLE="page-break-after: always;"></div>
## 2. DataStream API简介

### 2.1 什么能被转化为流

Flink 的 Java 和 Scala DataStream API 可以将任何可序列化的对象转化为流。Flink 自带的序列化器有

- 基本类型，即 String、Long、Integer、Boolean、Array
- 复合类型：Tuples、POJOs 和 Scala case classes

而且 Flink 会交给 Kryo 序列化其他类型。也可以将其他序列化器和 Flink 一起使用。特别是有良好支持的 Avro。

> Avro是一个数据序列化系统，设计用于支持大批量数据交换的应用。它的主要特点有：支持二进制序列化方式，可以便捷，快速地处理大量数据；动态语言友好，Avro提供的机制使动态语言可以方便地处理Avro数据。

### 2.2 Java tuple 和 POJOS

**Flink 的原生序列化器可以高效地操作 tuples 和 POJOs**

在通过Flink算子进行一些转换操作时(join，coGroup，keyBy，groupBy)，我们要求在操作的元素结合上定义键。另外一些转换操作(Reduce, GroupReduce, Aggregate, Windows)允许在应用这些转换之前将数据按键分组。

如下对 DataSet 分组

```java
DataSet<...> input = // [...]
DataSet<...> reduced = input
  .groupBy(/*在这里定义键*/)
  .reduceGroup(/*一些处理操作*/);
```

如下对 DataStream 指定键

```java
DataStream<...> input = // [...]
DataStream<...> windowed = input
  .keyBy(/*在这里定义键*/)
  .window(/*指定窗口*/);
```

Flink 的数据模型不是基于键值对的。因此你不需要将数据集类型物理地打包到键和值中。键都是“虚拟的”：它们的功能是指导分组算子用哪些数据来分组。

#### 2.2.1 Tuples

**对于 Java，Flink 自带有 `Tuple0` 到 `Tuple25` 类型。Flink tuples 是固定长度固定类型的Java Tuple实现**， 其长度从1到25分别对应不通的Tuple。形式有点类似于Python的元组，但是Flink对每一个tuple都限定了元组的长度。

```java
Tuple2<String, Integer> person = Tuple2.of("Fred", 35);

// zero based index!  
String name = person.f0;
Integer age = person.f1;
```

**如何为tuple元素定义键**

==**最简单的方式就是将tuple的一个或者多个字段定义为键来进行分组：**==

**==1. 按照一个字段(整型字段)对tuple分组==**

```java
DataStream<Tuple3<Integer,String,Long>> input = // [...]
KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0)
```

**==2. 用第一个字段和第二个字段组成的组合键对 Tuple 分组==**

```java
DataStream<Tuple3<Integer,String,Long>> input = // [...]
KeyedStream<Tuple3<Integer,String,Long>,Tuple> keyed = input.keyBy(0,1)
```

**==对于嵌套 Tuple 请注意： 如果你的 DataStream 是嵌套 Tuple，例如：==**

```java
DataStream<Tuple3<Tuple2<Integer, Float>,String,Long>> ds;
```

指定 `keyBy(0)` 将导致系统使用整个 `Tuple2` 作为键（一个整数和一个浮点数）。 如果你想“进入”到 `Tuple2` 的内部，你必须使用如下所述的字段表达式键POJOS。

可以使用基于字符串的字段表达式来引用嵌套字段，并定义用于分组、排序、join 或 coGrouping 的键。

#### 2.2.2 POJOS

字段表达式可以很容易地选取复合（嵌套）类型中的字段，例如 [Tuple](https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/api_concepts.html#tuples-and-case-classes) 和 [POJO](https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/api_concepts.html#pojos) 类型。

下例中，我们有一个包含“word”和“count”两个字段的 POJO：`WC`。要用 `word` 字段分组，我们只需要把它的名字传给 `keyBy()` 函数即可。

```java
// 普通的 POJO（简单的 Java 对象）
public class WC {
  public String word;
  public int count;
}
DataStream<WC> words = // [...]
DataStream<WC> wordCounts = words.keyBy("word").window(/*指定窗口*/);
```

**字段表达式语法**：

- 根据字段名称选择 POJO 的字段。例如 `“user”` 就是指 POJO 类型的“user”字段。
- 根据字段名称或 0 开始的字段索引选择 Tuple 的字段。例如 `“f0”` 和 `“5”` 分别指 Java Tuple 类型的第一个和第六个字段。
- 可以选择 POJO 和 Tuple 的嵌套字段。 例如，一个 POJO 类型有一个“user”字段还是一个 POJO 类型，那么 `“user.zip”` 即指这个“user”字段的“zip”字段。任意嵌套和混合的 POJO 和 Tuple都是支持的，例如 `“f1.user.zip”` 或 `“user.f3.1.zip”`。
- 可以使用 `"*"` 通配符表达式选择完整的类型。这也适用于非 Tuple 或 POJO 类型。

**字段表达式示例**:

```java
public static class WC {
  public ComplexNestedClass complex; //嵌套的 POJO
  private int count;
  // 私有字段（count）的 getter 和 setter
  public int getCount() {
    return count;
  }
  public void setCount(int c) {
    this.count = c;
  }
}
public static class ComplexNestedClass {
  public Integer someNumber;
  public float someFloat;
  public Tuple3<Long, Long, String> word;
  public IntWritable hadoopCitizen;
}
```

这些字段表达式对于以上代码示例都是合法的：

- `"count"`：`WC` 类的 count 字段。
- `"complex"`：递归选择 POJO 类型 `ComplexNestedClass` 的 complex 字段的全部字段。
- `"complex.word.f2"`：选择嵌套 `Tuple3` 类型的最后一个字段。
- `"complex.hadoopCitizen"`：选择 hadoop 的 `IntWritable` 类型。

通过上述例子，可以看出==**普通的POJOS类型就是一个简单的Java对象，但是并不是所有的Java对象都可以识别为POJO**==。**Flink对POJO进行了如下限制：**

如果满足以下条件，Flink 将数据类型识别为 POJO 类型（并允许“按名称”字段引用）：

- 该类是公有且独立的（没有非静态内部类）
- 该类有公有的无参构造函数
- 类（及父类）中所有的所有不被 static、transient 修饰的属性要么是公有的（且不被 final 修饰），要么是包含公有的 getter 和 setter 方法，这些方法遵循 Java bean 命名规范。例如一个名为 `foo` 的字段，它的 getter 和 setter 方法必须命名为 `getFoo()` 和 `setFoo()`。
- 字段的类型必须被已注册的序列化程序所支持。

POJO 通常用 `PojoTypeInfo` 表示，并使用 `PojoSerializer`（[Kryo](https://github.com/EsotericSoftware/kryo) 作为可配置的备用序列化器）序列化。 例外情况是 POJO 是 Avro 类型（Avro 指定的记录）或作为“Avro 反射类型”生成时。 在这种情况下 POJO 由 `AvroTypeInfo` 表示，并且由 `AvroSerializer` 序列化。 如果需要，你可以注册自己的序列化器；更多信息请参阅 [序列化](https://ci.apache.org/projects/flink/flink-docs-master/zh/dev/types_serialization.html)。

Flink 分析 POJO 类型的结构，也就是说，它会推断出 POJO 的字段。因此，POJO 类型比常规类型更易于使用。此外，Flink 可以比一般类型更高效地处理 POJO。

下例展示了一个拥有两个公有字段的简单 POJO。

示例：

```java
public class WordWithCount {

    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}

DataStream<WordWithCount> wordCounts = env.fromElements(
    new WordWithCount("hello", 1),
    new WordWithCount("world", 2));

wordCounts.keyBy("word"); // 以字段表达式“word”为键
```

### 2.3 一个完整的示例

这是Apache Flink官网的[示例](https://ci.apache.org/projects/flink/flink-docs-release-1.11/zh/learn-flink/datastream_api.html)，示例通过将人的名字和年龄的二元组作为数据流输入，然后通过算子过滤后仅输出成年人的元组

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;

public class Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        DataStream<Person> adults = flintstones.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
                return person.age >= 18;
            }
        });

        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;
        public Person() {};

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        };

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        };
    }
}
```

**==输出如下：==**

```markdown
9> Fred: age 35
10> Wilma: age 35
```

### 2.4 Stream执行环境

#### 2.4.1 执行流程

每个 Flink 应用都需要有执行环境，在该示例中为 `env`。流式应用需要用到 `StreamExecutionEnvironment`。

DataStream API 将你的应用构建为一个 job graph，并附加到 `StreamExecutionEnvironment` 。当调用 `env.execute()` 时此 graph 就被打包并发送到 JobManager 上，后者对作业并行处理并将其子任务分发给 Task Manager 来执行。每个作业的并行子任务将在 *task slot* 中执行。

**==注意，如果没有调用 execute()，应用就不会运行。==**

![image-20201209195653586](D:\Markdown\images\Flink\image-20201209195653586.png)

**==此分布式运行时取决于你的应用是否是可序列化的。它还要求所有依赖对集群中的每个节点均可用。==**

#### 2.4.2 Stream Source

上述示例用 `env.fromElements(...)` 方法构造 `DataStream<Person>` 。这样将简单的流放在一起是为了方便用于原型或测试。`StreamExecutionEnvironment` 上还有一个 `fromCollection(Collection)` 方法。因此，你可以这样做：

```java
List<Person> people = new ArrayList<Person>();

people.add(new Person("Fred", 35));
people.add(new Person("Wilma", 35));
people.add(new Person("Pebbles", 2));

DataStream<Person> flintstones = env.fromCollection(people);
```

另一个获取数据到流中的便捷方法是用 socket

```java
DataStream<String> lines = env.socketTextStream("localhost", 9999)
```

或读取文件

```java
DataStream<String> lines = env.readTextFile("file:///path");
```

在真实的应用中，最常用的数据源是那些支持低延迟，高吞吐并行读取以及重复（高性能和容错能力为先决条件）的数据源，例如 Apache Kafka，Kinesis 和各种文件系统。REST API 和数据库也经常用于增强流处理的能力（stream enrichment）。

#### 2.4.3 Stream sink

上述示例用 `adults.print()` 打印其结果到 task manager 的日志中（如果运行在 IDE 中时，将追加到你的 IDE 控制台）。它会对流中的每个元素都调用 `toString()` 方法。

输出看起来类似于

```
1> Fred: age 35
2> Wilma: age 35
```

1> 和 2> 指出输出来自哪个线程 sub-task（即 thread）

在生产中，常用的Sink包括StreamingFileSink、各种数据库和若干pub-sub系统。

### 2.5 动手练习

**==Apache Flink官网实践练习：==** **克隆 [flink-training repo](https://github.com/apache/flink-training/tree/release-1.11) 并在阅读完 README 中的指示后，开始尝试第一个练习吧： [Filtering a Stream (Ride Cleansing)](https://github.com/apache/flink-training/tree/release-1.11/ride-cleansing)。**

PS:

1.建议将代码下载到本地后，将需要练习的类代码复制到自己的项目中

2.如果你使用的是maven工程，当运行测试代码时显示缺少对应的类可能是因为在pom.xml文件中引入了`<scope>provided</scope>`的依赖，此时可通过以下方式解决：

- IDEA中右上角run图标左边下拉框的Edit Configurations，勾选Include dependencies with “Provided” scope.
- 将pom.xml依赖中provided依赖改为compile

成功运行`RideCleansingExercise.java`可以看到如下运行结果，对DataStream进行过滤，`GeoUtils`工具类提供了一个静态方法`isInNYC(float lon, float lat)`。来检查位置是纽约市的区域内通过经纬度信息判断骑行的起点和终点都在纽约市区内的记录

![image-20201211151656395](C:\Users\z00572820\AppData\Roaming\Typora\typora-user-images\image-20201211151656395.png)

3.如果发现程序抛出MissingSolutionException异常，是因为程序NYCFilter()类中的定义，此时可以修改正确的处理方式，或者粗暴的改为return false；

<div STYLE="page-break-after: always;"></div>
## 3. 数据通道 & ETL

> Apache Flink 的一种常见应用场景是 ETL（抽取、转换、加载）管道任务。从一个或多个数据源获取数据，进行一些转换操作和信息补充，将结果存储起来。

本节中的示例建立在你已经熟悉 [flink-training repo](https://github.com/apache/flink-training/tree/release-1.11) 中的出租车行程数据的基础上。我们在2.5中使用的练习类如下

**==RideCleansingExercise.java==**

```java
public class RideCleansingExercise extends ExerciseBase {

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        // print the filtered stream
        printOrTest(filteredRides);

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    private static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            //通过经纬度判断骑行起点和终点是否在纽约市
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
                    GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }
}
```

### 3.1 无状态转换

#### 3.1.1 map()

在2.5章节的实践练习当中，对数据流的过滤有一个 `GeoUtils` 类，提供了一个静态方法 `GeoUtils.mapToGridCell(float lon, float lat)`，它可以将位置坐标（经度，维度）映射到 100x100 米的对应不同区域的网格单元。

**现在让我们为每个出租车行程时间的数据对象增加 `startCell` 和 `endCell` 字段。你可以创建一个继承 `TaxiRide` 的 `EnrichedRide` 类，添加这些字段：**

```java
public static class EnrichedRide extends TaxiRide {
    public int startCell;
    public int endCell;

    public EnrichedRide() {}

    public EnrichedRide(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        ...
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    public String toString() {
        return super.toString() + "," +
            Integer.toString(this.startCell) + "," +
            Integer.toString(this.endCell);
    }
}
```

**在2.5章节中，使用了filter对数据进行过滤，然后对过滤后的数据我们来进行map转换**

```java
DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(...));

DataStream<EnrichedRide> enrichedNYCRides = rides
    .filter(new RideCleansingSolution.NYCFilter())
    .map(new Enrichment()); //map Transformation

enrichedNYCRides.print();
```

**Enrichment() 实现了接口MapFunction**

```java
public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {

    @Override
    public EnrichedRide map(TaxiRide taxiRide) throws Exception {
        return new EnrichedRide(taxiRide);
    }
}
```

#### 3.1.2 flatmap()

==**`MapFunction` 只适用于一对一的转换：对每个进入算子的流元素，`map()` 将仅输出一个转换后的元素。对于除此以外的场景，你将要使用 `flatmap()`。**==

```java
DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(...));

DataStream<EnrichedRide> enrichedNYCRides = rides
    .flatMap(new NYCEnrichment());

enrichedNYCRides.print();
```

其中用到的 `FlatMapFunction` :

```java
public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {

    @Override
    public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
        FilterFunction<TaxiRide> valid = new RideCleansing.NYCFilter();
        if (valid.filter(taxiRide)) {
            out.collect(new EnrichedRide(taxiRide));
        }
    }
}
```

使用接口中提供的 `Collector` ，`flatmap()` 可以输出你想要的任意数量的元素，也可以一个都不发。

### 3.2 Keyed Streams

#### 3.2.1 KeyBy()

将一个流根据其中的一些属性来进行分区是十分有用的，这样我们可以使所有具有相同属性的事件分到相同的组里。例如，如果你想找到从每个网格单元出发的最远的出租车行程。按 SQL 查询的方式来考虑，这意味着要对 `startCell` 进行 GROUP BY 再排序，在 Flink 中这部分可以用 `keyBy(KeySelector)` 实现。

```java
DataStream<EnrichedRide> enrichedNYCRides = rides
    .flatMap(new NYCEnrichment())
    .keyBy(enrichedRide -> enrichedRide.startCell)
```

每个 `keyBy` 会通过 shuffle 来为数据流进行重新分区。总体来说这个开销是很大的，它涉及网络通信、序列化和反序列化。

![image-20201214154635753](../../images/Flink/image-20201214154635753.png)

#### 3.2.2 通过计算获取键

Flink不仅仅支持通过`KeyBy`去直接获取事件中的某一元素作为键，同样也支持通过计算得出的结果作为键，并且实现了 `hashCode()` 和 `equals()`。这些限制条件不包括产生随机数或者返回 Arrays 或 Enums 的 KeySelector，但你可以用元组和 POJO 来组成键，只要他们的元素遵循上述条件。

键必须按确定的方式产生，因为它们会在需要的时候被重新计算，而不是一直被带在流记录中。

例如，比起创建一个新的带有 `startCell` 字段的 `EnrichedRide` 类，用这个字段作为 key：

```java
keyBy(enrichedRide -> enrichedRide.startCell)
```

我们更倾向于这样做：

```java
keyBy(ride -> GeoUtils.mapToGridCell(ride.startLon, ride.startLat))
```

**==示例：==**假设我们通过`map()`将`EnrichedRide`的初始经纬度和结束经纬度映射成了 `startCell` 和 `endCell` ，现在我们通过`KeyBy`将初始和结束Cell相差5000以上的分为一组，其他的分为另外一组：

```java
DataStream<EnrichedRide> filterRides = taxiRide
    .filter(new NYCFilter())
    .map(new Enrichment())
    .keyBy(value -> Math.abs(value.gridTuple.f0 - value.gridTuple.f1) > 5000);
```

输出结果如下可以看到两组在不同的线程里面输出：

```
8> (45886,19460)
1> (47382,43144)
8> (45138,31427)
8> (46385,26441)
8> (22701,42147)
1> (23200,24446)
8> (35166,43642)
1> (36413,37659)
8> (34667,25693)
8> (21953,49127)
8> (48130,30679)
1> (33421,32175)
8> (21704,38906)
1> (20707,19959)
8> (44639,19461)
```

#### 3.2.3 聚合

以下代码为每个行程`EnrichedRide`结束事件创建了一个新的包含 `startCell` 和时长（分钟）的元组流：

```java
import org.joda.time.Interval;

DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
    .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {

        @Override
        public void flatMap(EnrichedRide ride,
                            Collector<Tuple2<Integer, Minutes>> out) throws Exception {
            if (!ride.isStart) {
                Interval rideInterval = new Interval(ride.startTime, ride.endTime);
                Minutes duration = rideInterval.toDuration().toStandardMinutes();
                out.collect(new Tuple2<>(ride.startCell, duration));
            }
        }
    });
```

现在就可以产生一个流，对每个 `startCell` 仅包含那些最长行程的数据。

有很多种方法表示使用哪个字段作为键。前面使用 `EnrichedRide` POJO 的例子，用字段名来指定键。而这个使用 `Tuple2` 对象的例子中，用字段在元组中的序号（从0开始）来指定键。

```java
minutesByStartCell
  .keyBy(value -> value.f0) // .keyBy(value -> value.startCell)
  .maxBy(1) // duration
  .print();
```

现在每次行程时长达到新的最大值，都会输出一条新记录，例如下面这个对应 50797 网格单元的数据：

```
...
4> (64549,5M)
4> (46298,18M)
1> (51549,14M)
1> (53043,13M)
1> (56031,22M)
1> (50797,6M)
...
1> (50797,8M)
...
1> (50797,11M)
...
1> (50797,12M)
```

## 4. 流式分析

### 4.1 时间语义

#### 4.1.1 概要

Flink 明确支持以下三种时间语义:

- *事件时间(event time)：* 事件产生的时间，记录的是设备生产(或者存储)事件的时间
- *摄取时间(ingestion time)：* Flink 读取事件时记录的时间
- *处理时间(processing time)：* Flink pipeline 中具体算子处理事件的时间

为了获得可重现的结果，例如在计算过去的特定一天里第一个小时股票的最高价格时，我们应该使用事件时间。这样的话，无论什么时间去计算都不会影响输出结果。然而如果使用处理时间的话，实时应用程序的结果是由程序运行的时间所决定。多次运行基于处理时间的实时程序，可能得到的结果都不相同，也可能会导致再次分析历史数据或者测试新代码变得异常困难。

#### 4.1.2 使用 Event Time

​		如果想要使用事件时间，需要额外给 Flink 提供一个时间戳提取器和 Watermark 生成器（水位线），Flink 将使用它们来跟踪事件时间的进度。

