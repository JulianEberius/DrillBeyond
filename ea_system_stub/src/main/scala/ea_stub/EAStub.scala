package ea_stub

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.Random
import spark._
import spark.Spark._

abstract class JsonTransformer(path: String, contentType: String)
  extends ResponseTransformerRoute(path, contentType) {
  override def render(model: AnyRef): String = model match {
    case j: JSONSerializable => j.toJson
    case j => j.toString
  }
}

object EAStub {

  val indexDir = null
  val rnd = new Random()
  var selectivity = 1.0
  var fuzziness = 0.0

  def setup(args: Array[String]) = {
    var port = 8765
    var indexPath:String = null

    if (args.size >= 1) {
      port = args(0).toInt
    }
    setPort(port)

    println("ARGS: "+args.mkString(" -- "))
    println("port: " + port)
  }

  case class DrillbeyondResponse(
    candidates:Array[DrillbeyondSolution],
    inUnion:Array[Boolean]) extends JSONSerializable // true if the entitiy fullfills the predicate in all candidate sources
  case class DrillbeyondSolution(values:Array[java.lang.Double], selectivity:Double)


  def unionSelect(pred:Option[Predicate], solutions: Seq[DrillbeyondSolution]): Array[Boolean] = {
    val n = solutions(0).values.length
    pred match {
      case None => (0 until n).map(_=>true).toArray
      case Some(p) =>
        (0 until n).map(i =>
          solutions
            .map(_.values(i))
            .exists(v_i => p.evaluate(v_i))) // pick all that are selected with any candidate
            .toArray
    }
  }

  def main(args: Array[String]) {
    setup(args)
    
    post(new JsonTransformer("/artificial", "application/json") {
      override def handle(request: Request, response: Response) = {
        val drb_req = JSONSerializable.fromJson(classOf[DrillbeyondRequest], request.body)
        val entities = drb_req.columns(0)

        val pred = drb_req.predicates.find(a => a.isInstanceOf[NumericValuedPredicate]).asInstanceOf[Option[NumericValuedPredicate]]
        val output = if (pred.isEmpty) {
          val candidates = (0 until drb_req.max_cands).map(i => {
            val vals:Array[java.lang.Double] = entities.map(e =>  new java.lang.Double(new Random(e.hashCode + 1000000*i).nextDouble))
            DrillbeyondSolution(vals, 1.0)
          })
          DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        }
        else
        {
          val predVal:Double = pred.get.value
          val candidates = (0 until drb_req.max_cands).map(i => {

            val vals = pred.get.artificial_values(entities, selectivity, (i+1)*10000, fuzziness)
            DrillbeyondSolution(
              vals,
              drb_req.predicates(0).selectivity(vals))
            })
          DrillbeyondResponse(candidates.toArray, unionSelect(pred, candidates))
        }
        val unionSelectivity = output.inUnion.count(_ == true) / output.inUnion.size.toDouble
        output
      }
    })
    get(new Route("/set_selectivity/:sel") {
      override def handle(request: Request, response: Response) = {
        val reqSel = request.params(":sel")
        selectivity = reqSel.toDouble
        "Ok"
      }
    })
    get(new Route("/set_fuzziness/:fuz") {
      override def handle(request: Request, response: Response) = {
        val reqFuzz = request.params(":fuz")
        fuzziness = reqFuzz.toDouble
        "Ok"
      }
    })
  }
}
