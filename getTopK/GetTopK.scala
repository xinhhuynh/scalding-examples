import com.twitter.scalding._

// Gets the top k records from an input file, ordered by a column (of type integer).
class GetTopK(args : Args) extends Job(args) {
  Tsv(args("input"), ('keyField, 'numField))
    // Tsv reads in columns as strings; we want the second field to be an Int.
    .map ('numField-> 'numFieldInt) {x:Int => x}
    .project('keyField, 'numFieldInt)
    // This does the right thing by taking the top k in the mapper before reducing.
    .groupAll{_.sortedReverseTake[(Int, String)](('numFieldInt, 'keyField) -> 'top, args("topK").toInt)} 
    .flattenTo[(Int,String)]('top -> ('numFieldInt, 'keyField))
    .project(('keyField, 'numFieldInt))
    .write(Tsv(args("output")))
}
