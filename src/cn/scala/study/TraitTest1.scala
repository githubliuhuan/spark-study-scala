package cn.scala.study


// 在Scala中，类继承Trait后，必须实现其中的抽象方法，实现时不需要使用override关键字，同时Scala同Java一样，不支持类多继承，但支持多重继承Trait，使用with关键字即可。
trait HelloTrait {
  def sayHello(name: String)
}

trait MakeFriendsTrait {
  def makeFriends(p: Person)
}

class Person(val name: String) extends HelloTrait with MakeFriendsTrait {
  def sayHello(name: String) = println("Hello," + name)
  def makeFriends(p: Person) = println("Hello," +p.name + ",my name is " + name)
}

object HelloPerson {
  def main(args: Array[String]): Unit = {
    val p = new Person("lindan")
    p.sayHello("lindan")

    val p1 = new Person("lizongwei")

    p.makeFriends(p1)
  }
}