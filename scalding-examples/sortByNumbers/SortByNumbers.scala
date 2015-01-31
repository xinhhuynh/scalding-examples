import com.twitter.scalding._

// Sorts an input file by a numerical field in descending order.
class SortByNumbers(args : Args) extends Job(args) {
  Tsv(args("input"), ('keyField, 'numField))
    // Tsv reads in columns as strings; we want the second field to be an Int.
    .map ('numField-> 'numFieldInt) {x:Int => x}
    .project('keyField, 'numFieldInt)
    .groupAll{ _.sortBy('numFieldInt).reverse } 
    .write(Tsv(args("output")))
}
