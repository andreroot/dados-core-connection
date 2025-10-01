resource "aws_s3_bucket" "this" {
  bucket = "ephemeral-emr-config-safira"
}

# upload jars
resource "aws_s3_object" "jars" {
  for_each = fileset("../safira_emr/utils/jars/", "*")
  bucket   = aws_s3_bucket.this.id
  key      = "jars/${each.value}"
  source   = "../safira_emr/utils/jars/${each.value}"
  etag     = filemd5("../safira_emr/utils/jars/${each.value}")
}

# upload install_libraries.sh
resource "aws_s3_object" "install_libraries" {
  bucket   = aws_s3_bucket.this.id
  key      = "bootstrap/install_libraries.sh"
  source   = "../safira_emr/utils/install_libraries.sh"
  etag     = filemd5("../safira_emr/utils/install_libraries.sh")
}