// Databricks notebook source exported at Sun, 7 Feb 2016 04:18:57 UTC
dbutils.fs.mounts()

// COMMAND ----------

dbutils.fs.ls("/mnt/kd_traffic")

// COMMAND ----------

// run once after cluster startup
val AccessKey = getArgument("1. ACCESS_KEY", "enter access key")
val SecretKey = getArgument("2. SECRET_KEY", "enter secret key")

// COMMAND ----------

import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.OutputStreamWriter

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.annotation.Since
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.ObjectMetadata

import org.ucsd.dse.capstone.traffic._

// COMMAND ----------

def execute(client: AmazonS3, m_vector_rdd: RDD[Vector], obs_name: String, fid: String, file_dir_prefix: String) = {
  //
  // obtain mean vector
  //
  val m_summary_stats: MultivariateStatisticalSummary = MLibUtils.summary_stats(m_vector_rdd)
  val mean_vector = m_summary_stats.mean.toArray
  val mean_filename = file_dir_prefix + "mean_vector." + fid + ".csv"
  val mean_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
  MLibUtils.write_vectors(mean_filename, List[Vector](Vectors.dense(mean_vector)), filename => {
    new BufferedWriter(new OutputStreamWriter(mean_stream))
  })
  val mean_stream_tuple = (mean_filename, mean_stream)
  //
  // execute PCA
  //
  val m_pca_vector_rdd: RDD[Vector] = m_vector_rdd
  val k = 30
  val (eigenvectors, eigenvalues) = MLibUtils.execute_pca(m_pca_vector_rdd, k)
  //
  // eigenvectors written out as column-major matrix
  //
  val eigenvectors_filename = file_dir_prefix + "eigenvectors." + fid + ".csv"
  val eigenvector_stream: ByteArrayOutputStream = new ByteArrayOutputStream();
  MLibUtils.write_matrix(eigenvectors_filename, eigenvectors, filename => {
    new BufferedWriter(new OutputStreamWriter(eigenvector_stream))
  })
  val eigenvector_stream_tuple = (eigenvectors_filename, eigenvector_stream)
  //
  // eigenvalues written out as one row
  //
  val eigenvalue_filename = file_dir_prefix + "eigenvalues." + fid + ".csv"
  val eigenvalue_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
  MLibUtils.write_vectors(eigenvalue_filename, List[Vector](eigenvalues), filename => {
    new BufferedWriter(new OutputStreamWriter(eigenvalue_stream))
  })
  val eigenvalue_stream_tuple = (eigenvalue_filename, eigenvalue_stream)
  //
  // take a sample of 10 vectors
  //
  val sample_arr: Array[Vector] = m_vector_rdd.takeSample(false, 10, 47)
  val sample_filename = file_dir_prefix + "samples." + fid + ".csv"
  val sample_stream: ByteArrayOutputStream = new ByteArrayOutputStream()
  MLibUtils.write_vectors(sample_filename, sample_arr, filename => {
    new BufferedWriter(new OutputStreamWriter(sample_stream))
  })
  val sample_stream_tuple = (sample_filename, sample_stream)
  //
  // write streams to files
  //
  val tuple_list: List[Tuple2[String, ByteArrayOutputStream]] = List[Tuple2[String, ByteArrayOutputStream]](mean_stream_tuple, eigenvector_stream_tuple, eigenvalue_stream_tuple, sample_stream_tuple)
  tuple_list.foreach { tuple: Tuple2[String, ByteArrayOutputStream] =>
    val filename: String = tuple._1
    val stream: ByteArrayOutputStream = tuple._2
    //
    val output_bucket_name = "dse-team2-2014"
    val simple_filename = filename.split("/").last
    val output_bucket_key = "output/" + obs_name + "_" + simple_filename
    //
    val bytes: Array[Byte] = stream.toByteArray();
    //
    val in_stream: InputStream = new ByteArrayInputStream(bytes)
    val in_stream_meta: ObjectMetadata = new ObjectMetadata()
    in_stream_meta.setContentLength(bytes.length)
    //
    println("Invoking client.putObject with parameters: %s,%s".format(output_bucket_name, output_bucket_key))
    client.putObject(output_bucket_name, output_bucket_key, in_stream, in_stream_meta)
  }
  //
  // print statements to verify
  //
  println("eigenvectors= " + eigenvectors)
  println("eigenvalues= " + eigenvalues)
  val m_list_buffer = new ListBuffer[Double]()
  val m_eig_arr: Array[Double] = eigenvalues.toArray
  var cum_sum = 0.0
  for (i <- 0 to m_eig_arr.length - 1) {
    cum_sum += m_eig_arr(i)
    m_list_buffer += cum_sum
  }
  println("perc variance explained= " + m_list_buffer)
}

// COMMAND ----------

//
// Read DataFrame
//
val path = "/mnt/kd_traffic/pivot_output_2010"
val pivot_df = MLibUtils.read_pivot_df(sc, sqlContext, path)

// COMMAND ----------

val cred: AWSCredentials = new BasicAWSCredentials(AccessKey, SecretKey)
val client: AmazonS3 = new AmazonS3Client(cred)
//
// Execute PCA for each field
//
val m_column_prefixes = List((PivotColumnPrefixes.TOTAL_FLOW, "total_flow"), (PivotColumnPrefixes.SPEED, "speed"), (PivotColumnPrefixes.OCCUPANCY, "occupancy"))
val fid = "CA_2010"
val file_dir_prefix = "/mnt/kd_traffic/output/"; // end '/' required
m_column_prefixes.foreach { m_struct =>
  val column_prefix= m_struct._1
  val obs_name= m_struct._2
  val m_vector_rdd: RDD[Vector] = MLibUtils.to_vector_rdd(pivot_df, column_prefix)
  execute(client, m_vector_rdd, obs_name, fid, file_dir_prefix)
}

// COMMAND ----------

