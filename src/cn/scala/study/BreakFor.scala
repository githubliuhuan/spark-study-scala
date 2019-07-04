package cn.scala.study

// 跳出循环语句的3种方法
object BreakFor {

  def main(args: Array[String]): Unit = {
    // 方法一 使用boolean变量
//    var flag = true
//    var res = 0
//    var n = 0
//    while (flag) {
//      res += n
//      n += 1
//
//      if (n == 5) {
//        flag = false
//      }
    }
//    print(res)


// 高级for循环 加上了if守卫
//    var flag = true
//    var res = 0
//    var n = 10
//
//    for (i <- 0 until 10 if flag) {
//      res += i
//      if (i == 4){
//        flag = false
//        println(res)
//      }

//    }


// 在嵌套函数中使用return
//  def add_outer() = {
//    var res = 0
//
//    def add_inner(): Unit ={
//      for (i <- 0 until 10) {
//        if (i == 5){
//          return
//        }
//        res += i
//      }
//    }
//
//    add_inner()
//    res
//  }


// 使用Breaks对象的break方法
import scala.util.control.Breaks._

  var res = 0
  breakable{
    for(i <- 0 until 10){
      if(i == 5){
        break

      }
      res += i
    }
  }

  }