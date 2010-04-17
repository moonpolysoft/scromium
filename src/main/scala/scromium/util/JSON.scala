package scromium.util

import scala.util.parsing.combinator._

object JSON extends JavaTokenParsers {
  def parseObject(json : String) : Map[String, Any] = {
    parseAll(obj, json).get
  }
  
  def obj: Parser[Map[String, Any]] = 
    "{"~> repsep(member, ",") <~"}" ^^ (Map() ++ _) 

  def arr: Parser[List[Any]] = 
    "["~> repsep(value, ",") <~"]" 

  def member: Parser[(String, Any)] = 
    stringLiteral~":"~value ^^ 
            { case name~":"~value => (name.substring(1, name.length-1), value) } 

  def value: Parser[Any] = ( 
    obj 
    | arr 
    | stringLiteral ^^ (x => x.substring(1,x.length - 1))
    | floatingPointNumber ^^ (_.toInt) 
    | "null" ^^ (x => null) 
    | "true" ^^ (x => true) 
    | "false" ^^ (x => false) 
    )
                
        
}