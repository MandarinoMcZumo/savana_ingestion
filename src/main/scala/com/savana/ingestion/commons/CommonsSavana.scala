
package org.svn.ingestion
package com.savana.ingestion.commons

trait CommonsSavana extends EnumsSavana {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  /**
   * Function to execute more than one method simultaneously
   * @param klass Class in which it is executed
   */
  case class Caller[T>:Null<:AnyRef](klass:T) {
    def call(methodName:String,args:AnyRef*):AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass,args: _*)
    }
    def exist(methodName:String,args:AnyRef*):Boolean = {
      klass.getClass.getMethods.exists(x => x.getName.equalsIgnoreCase(methodName))
    }
  }


}
