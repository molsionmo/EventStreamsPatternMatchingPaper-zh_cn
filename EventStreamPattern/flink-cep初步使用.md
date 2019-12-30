# Flink cep的初步使用

## 一、CEP是什么

在应用系统中，总会发生这样或那样的事件，有些事件是用户触发的，有些事件是系统触发的，有些可能是第三方触发的，但它们都可以被看做系统中可观察的状态改变，例如用户登陆应用失败、用户下了一笔订单或RFID传感器回报的消息。应对状态改变的策略可以分为两类，一类是简单事件处理（Simple event processing），一般简单事件处理会有两个步骤，过滤和路由，决定是否要处理，由谁处理，另一类是复杂事件处理（Complex event processing），复杂事件处理本身也会处理单一的事件，但其典型特质是需要对多个事件组成的是事件流进行检测分析并响应。

在维基百科中也对CEP做了定义，“CEP是一种事件处理模式，它从若干源中获取事件，并侦测复杂环境的事件或模式，CEP的目的是确认一些有意义的事件（比如某种威胁或某种机会），并尽快对其作出响应”，可见CEP的主要特点包括：复杂性，需要在多源的事件流中进行检测；低延迟，秒级或毫秒级的响应，比如应对威胁；高吞吐，需要迅速对大量或者超大量事件流作出响应。

## 二、Flink CEP

Flink作为目前大数据领域实时计算的主流计算框架，天然支持低延迟、高吞吐等特性，再加上Flink中的窗口模型和状态模型，更是对CEP提供了非常强大的支撑。Flink中专门实现了复杂事件处理的库——Flink CEP，用来方便的进行在事件流中检测事件模式。

以下是一个简单的例子，说明在Flink中如何实现CEP：

```java
public class CEPMain2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple3<Integer, String, String>> eventStream = env.fromElements(
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(982, "login", "fail"),
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(1320, "login", "success"),
                Tuple3.of(1500, "login", "fail"),
                Tuple3.of(982, "login", "fail"),
                Tuple3.of(1450, "exit", "success"),
                Tuple3.of(982, "login", "fail"),
                Tuple3.of(982, "login", "success"));
        //AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<Tuple3<Integer, String, String>, ?> loginFail =
                Pattern.<Tuple3<Integer, String, String>>begin("begin")
                        .where(new SimpleCondition<Tuple3<Integer, String, String>>() {
                            @Override
                            public boolean filter(Tuple3<Integer, String, String> s) throws Exception {
                                return s.f2.equalsIgnoreCase("fail");
                            }
                        }).times(3).within(Time.seconds(5))
                .followedBy("loginSuccess").where(new SimpleCondition<Tuple3<Integer, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<Integer, String, String> tuple3) throws Exception {
                        return tuple3.f1.equals("login") && tuple3.f2.equals("success");
                    }
                }).within(Time.seconds(5));
        PatternStream<Tuple3<Integer, String, String>> patternStream =
                CEP.pattern(eventStream.keyBy(x -> x.f0), loginFail);
        DataStream<String> alarmStream =
                patternStream.select(new PatternSelectFunction<Tuple3<Integer, String, String>, String>() {
                    @Override
                    public String select(Map<String, List<Tuple3<Integer, String, String>>> map) throws Exception {
                        log.info("p: {}", map);
                        String msg = String.format("ID %d has login failed 3 times in 5 seconds,but login success once in next 5 seconds."
                                , map.values().iterator().next().get(0).f0);
                        return msg;
                    }
                });

        alarmStream.print();

        env.execute("cep event test");
    }
}
```

运行结果如下：
![20191228165253.png](https://i.loli.net/2019/12/28/RPBXLxqUKr2l563.png)

可见成功捕获了ID为982的用户,5s内登录失败了3次,但接下来5s又成功登录了一次.

在Flink中实现一个CEP可以总结为四步：

* 一，构建需要的数据流
* 二，构造正确的模式，
* 三，将数据流和模式进行结合
* 四，在模式流中获取匹配到的数据

其中第一步和第三步一般会是标准操作，核心在于第二部构建模式，需要利用Flink CEP支持的特性，构造出正确反映业务需求的匹配模式。

## 三、Flink CEP中对CEP的支撑

Flink CEP的核心在于模式匹配，对于不同模式匹配特性的支持，往往决定相应的CEP框架是否能够得到广泛应用。Flink CEP对模式提供了如下的一些支持：

### （一）支持匹配模式

模式匹配具有一些共同的基础模式，对不同的模式匹配的语义的表达和实现，意味着这种模式能在多大范围内得到应用。

Flink CEP对模式匹配的语义支持具有如下特点：

1. 支持匹配数量，提供匹配次数的支持，可以指定匹配一次或多次（oneOrMore），可以指定匹配固定数量次（times(n)），也可以指定范围固定数量次（times(n,m)）。
2. 支持历史匹配，在匹配的过程中，既可以对当前事件进行属性判断，也可以对匹配事件组中的历史匹配结果进行回溯来进行判定。
3. 支持组匹配，支持将单模式进行组合成为模式组，支持不同的组合模式和语义，比如，or，until,begin，next，followBy，otNext，notFollowBy。
4. 支持窗口匹配，支持时间窗口，可以方便的在某个时间窗口内进行模式匹配。

### （二）支持不同临近条件

如果只是单模式匹配，则无需考虑临近条件。在模式组的执行中，即 多个模式组合执行的过程中，临近条件指的是如何将一组事件匹配到特定的模式组中的不同模式。不同的临近条件的使用，会显著的改变最终匹配的结果。

Flink CEP中支持如下三种临近条件：

* Strict Contiguity，严格临近指的是匹配事件必须具有严格的前后相邻关系，即匹配事件之间没有非匹配事件。
* Relaxed Contiguity，宽松匹配指的是匹配事件可以有非匹配事件，非匹配事件的存在不阻挡非连续事件被匹配成功。
* Non-Deterministic Relaxed Contiguity，非确定性宽松匹配在宽松匹配的基础上，即使某个事件被某个模式匹配完毕还可以参加后面其他模式的匹配。

### （三）支持不同匹配后策略

匹配后策略指的是当某一组事件成功匹配了某个模式之后，这组事件以何种方式参与后续的模式匹配。不同的匹配后策略会导致大相径庭的匹配结果，所以在实际开发中，需要小心的选择合适的匹配后策略。

Flink CEP支持如下五种匹配后策略：

* NO_SKIP策略，意即当前事件组中的事件还会不受约束的参与后续的模式匹配。
* SKIP_TO_NEXT策略，意即当前事件组中除了第一个事件之外，其他事件可以不受约束的参与后续的模式匹配。
* SKIP_PAST_LAST_EVENT策略，意即当前事件组中的任意一个事件都不参与后续的模式匹配。
* SKIP_TO_FIRST策略，此种策略需要指定一个模式，当前事件组中的任何子匹配如果包含指定模式匹配事件组中的最大匹配事件组，则此子匹配会被丢弃。
* SKIP_TO_LAST策略，此种策略需要指定一个模式，当前事件组中的任何子匹配如果包含指定模式匹配事件组中的最小匹配事件组，则此子匹配会被丢弃。

### （四）支持事件时间与乱序

在CEP的处理过程中，事件到达的顺序至关重要，因为事件到达的顺序会真正决定是否可以与相应的模式成功匹配。目前业界已有的CEP计算框架一般都采用事件到达的自然顺序，即处理时间作为模式匹配的基础，这种模式不能满足目前分布式环境下CEP的要求。在大数据分布式环境下，事件到达的顺序和事件发生的顺序往往不匹配，存在延迟到达和乱序等情况，这时往往需要依靠事件时间来进行相应的模式匹配，不然就会发生匹配错误或者匹配失效。

Flink特有的事件时间模型，包括event time特性与watermark机制同样可以在Flink CEP中发挥作用。Flink CEP会将事件进行缓存，不在一开始就进行模式匹配，在相应的watermark到底之后，Flink CEP将缓存中的事件按照事件时间进行排序，然后再进行相应的模式匹配，能够在很大程度上解决分布式环境下的CEP难题。
