package ea_stub

import scala.reflect.{ClassTag, classTag}
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.annotations.SerializedName
import com.google.common.collect.Sets
import java.lang.{Double => JDouble}
import com.google.gson.JsonSerializer
import scala.reflect.ClassTag
import com.google.gson.JsonPrimitive
import com.google.gson.JsonSerializationContext
import com.google.gson.JsonElement
import java.lang.reflect.Type
import java.math.BigDecimal

object JSONSerializable {
  class DoubleSerializer extends JsonSerializer[JDouble] {
      override def serialize(src:JDouble, typeOfSrc:Type, context: JsonSerializationContext):JsonElement = {
          val value = BigDecimal.valueOf(src)
          new JsonPrimitive(value.toPlainString())
      }
  }
  val gsonBuilder = new GsonBuilder()

  val gson  = gsonBuilder.create()
  def fromJson[C: ClassTag](t: Class[C], s: String): C = {
    JSONSerializable.gson.fromJson(s, classTag[C].runtimeClass)
  }
}

trait JSONSerializable {
  def toJson = {
    JSONSerializable.gson.toJson(this)
  }
}

class DrillbeyondRequest extends JSONSerializable {
  @SerializedName("columns") var columns: Array[Array[String]] = null
  @SerializedName("keyword") var keyword: String = null
  @SerializedName("max_cands") var max_cands: Int = 0
  @SerializedName("local_table") var concept: String = null
  @SerializedName("str_col_names") var col_names: Array[String] = null
  @SerializedName("restrictions") var restrictions: Array[String] = null
  
  lazy val predicates:Seq[Predicate] = restrictions.map(Predicate.fromString(_)).toSeq

}
