using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;
using Amazon.S3.Model;
using Google.Cloud.Storage.V1;
using Google.Cloud.Storage.V1; 
using Google.Apis.Auth.OAuth2;

// Simple console app demonstrating AWS S3 and Google Cloud Storage operations.
// Usage: dotnet run -- aws   OR   dotnet run -- gcp

async Task<int> Main(string[] args)
{
    if (args.Length == 0)
    {
        Console.WriteLine("Please pass provider: aws or gcp\nExample: dotnet run -- aws");
        return 1;
    }

    var provider = args[0].ToLowerInvariant();
    var baseName = "codtech-internship";
    var bucketName = GenerateBucketName(baseName);

    // local sample files
    var readmePath = Path.Combine(Directory.GetCurrentDirectory(), "readme.txt");
    var dataDir = Path.Combine(Directory.GetCurrentDirectory(), "data");
    Directory.CreateDirectory(dataDir);
    var exampleJsonPath = Path.Combine(dataDir, "example.json");

    if (!File.Exists(readmePath) || !File.Exists(exampleJsonPath))
    {
        Console.WriteLine("Sample files missing. Ensure readme.txt and data/example.json exist.");
        return 1;
    }

    if (provider == "aws")
    {
        Console.WriteLine("Running AWS S3 flow...");
        try
        {
            using var s3Client = new AmazonS3Client();
            await CreateBucketAwsIfNotExistsAsync(s3Client, bucketName);
            await UploadFileAwsAsync(s3Client, bucketName, "readme.txt", readmePath, true);
            await UploadFileAwsAsync(s3Client, bucketName, "data/example.json", exampleJsonPath, false);

            var presigned = GeneratePreSignedUrlAws(s3Client, bucketName, "data/example.json", TimeSpan.FromHours(1));
            Console.WriteLine($"Pre-signed URL (valid 1 hour): {presigned}");
            Console.WriteLine($"AWS bucket created: {bucketName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"AWS error: {ex.Message}");
            return 1;
        }
    }
    else if (provider == "gcp")
    {
        Console.WriteLine("Running GCP Storage flow...");
        try
        {
            var projectId = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT");
            if (string.IsNullOrEmpty(projectId))
            {
                Console.WriteLine("Set environment variable GOOGLE_CLOUD_PROJECT to your GCP project id.");
                return 1;
            }

            var storage = await StorageClient.CreateAsync();
            var bucket = await CreateBucketGcpIfNotExistsAsync(storage, projectId, bucketName);
            using (var fs = File.OpenRead(readmePath))
            {
                await storage.UploadObjectAsync(bucketName, "readme.txt", null, fs);
            }
            using (var fs = File.OpenRead(exampleJsonPath))
            {
                await storage.UploadObjectAsync(bucketName, "data/example.json", "application/json", fs);
            }

            // Generate a signed URL using service account credentials from GOOGLE_APPLICATION_CREDENTIALS
            var credsPath = Environment.GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS");
            if (string.IsNullOrEmpty(credsPath))
            {
                Console.WriteLine("Set GOOGLE_APPLICATION_CREDENTIALS to the service account JSON key path to generate signed URLs.");
                Console.WriteLine($"Public URL (may require ACL): https://storage.googleapis.com/{bucketName}/data/example.json");
            }
            else
            {
                var signer = UrlSigner.FromServiceAccountPath(credsPath);
                var signedUrl = signer.Sign(bucketName, "data/example.json", TimeSpan.FromHours(1), HttpMethod.Get);
                Console.WriteLine($"Signed URL (valid 1 hour): {signedUrl}");
            }

            Console.WriteLine($"GCP bucket created: {bucketName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"GCP error: {ex.Message}");
            return 1;
        }
    }
    else
    {
        Console.WriteLine("Unknown provider. Use aws or gcp.");
        return 1;
    }

    return 0;
}

string GenerateBucketName(string baseName)
{
    var suffix = Guid.NewGuid().ToString("n").Substring(0, 8);
    // bucket names must be lowercase and meet provider constraints
    return ($"{baseName}-{suffix}").ToLowerInvariant();
}

async Task CreateBucketAwsIfNotExistsAsync(IAmazonS3 client, string bucketName)
{
    if (!(await AmazonS3Util.DoesS3BucketExistV2Async(client, bucketName)))
    {
        var putRequest = new PutBucketRequest { BucketName = bucketName };
        await client.PutBucketAsync(putRequest);
        Console.WriteLine($"Created S3 bucket: {bucketName}");
    }
    else
    {
        Console.WriteLine($"S3 bucket already exists: {bucketName}");
    }
}

async Task UploadFileAwsAsync(IAmazonS3 client, string bucketName, string key, string filePath, bool makePublic)
{
    var tu = new TransferUtility(client);
    var request = new TransferUtilityUploadRequest
    {
        BucketName = bucketName,
        Key = key,
        FilePath = filePath,
    };
    if (makePublic)
        request.CannedACL = S3CannedACL.PublicRead;

    await tu.UploadAsync(request);
    Console.WriteLine($"Uploaded {key} to {bucketName} (public: {makePublic})");
}

string GeneratePreSignedUrlAws(IAmazonS3 client, string bucketName, string key, TimeSpan expires)
{
    var request = new GetPreSignedUrlRequest
    {
        BucketName = bucketName,
        Key = key,
        Expires = DateTime.UtcNow.Add(expires)
    };
    return client.GetPreSignedURL(request);
}

async Task<Google.Apis.Storage.v1.Data.Bucket> CreateBucketGcpIfNotExistsAsync(StorageClient client, string projectId, string bucketName)
{
    try
    {
        var existing = await client.GetBucketAsync(bucketName);
        Console.WriteLine("GCP bucket already exists: " + bucketName);
        return existing;
    }
    catch (Google.GoogleApiException e) when (e.HttpStatusCode == System.Net.HttpStatusCode.NotFound)
    {
        var bucket = await client.CreateBucketAsync(projectId, new Google.Apis.Storage.v1.Data.Bucket { Name = bucketName });
        Console.WriteLine("Created GCP bucket: " + bucketName);
        return bucket;
    }
}

return await Main(args);
