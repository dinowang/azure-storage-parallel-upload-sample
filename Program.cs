using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;

namespace Upload
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                                .SetBasePath(Directory.GetCurrentDirectory())
                                .AddJsonFile("appsettings.json", false)
                                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("NETCORE_ENVIRONMENT")}.json", optional: true)
                                .Build();

            var storageConnectionString = config.GetValue<string>("StorageConnectionString");
            var sourceUri = new Uri(config.GetValue<string>("SourceUri"));
            var sourceFile = Path.GetFileName(sourceUri.LocalPath);

            if (!File.Exists(sourceFile))
            {
                Console.WriteLine($"download sample file...");

                var httpClient = new HttpClient();
                using (var inputStream = await httpClient.GetStreamAsync(sourceUri))
                using (var outputStream = new FileStream(sourceFile, FileMode.Create, FileAccess.ReadWrite))
                {
                    await inputStream.CopyToAsync(outputStream);
                }
            }

            var filePath = sourceFile;
            var fileInfo = new FileInfo(filePath);
            var fileSize = fileInfo.Length;

            Console.WriteLine($"source:");
            Console.WriteLine($"\tfrom: {filePath}");

            /// Cloud Storage
            /// 
            var account = CloudStorageAccount.Parse(storageConnectionString);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference("file");
            await container.CreateIfNotExistsAsync();

            var stopwatch = new Stopwatch();

            /// Blob
            /// 
            Console.WriteLine($"\tsize: {fileSize:##,#} bytes");
            Console.WriteLine();

            /// Simple Upload
            /// 
            Console.WriteLine($"simple upload:");
            var blob = container.GetBlockBlobReference(sourceFile + ".simple");
            Console.WriteLine($"\tto: {blob.Uri}");
            if (await blob.ExistsAsync())
            {
                Console.WriteLine($"\tcleanup");
                await blob.DeleteIfExistsAsync();
            }
            stopwatch.Start();
            await blob.UploadFromFileAsync(filePath);
            stopwatch.Stop();
            Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
            Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
            Console.WriteLine();

            /// Simple Upload with BlobRequestOptions
            /// 
            Console.WriteLine($"simple upload with `ParallelOperationThreadCount`:");
            blob = container.GetBlockBlobReference(sourceFile + ".simple-parallel");
            Console.WriteLine($"\tto: {blob.Uri}");
            if (await blob.ExistsAsync())
            {
                Console.WriteLine($"\tcleanup");
                await blob.DeleteIfExistsAsync();
            }
            stopwatch.Restart();
            var parallelOptions = new BlobRequestOptions
            {
                ParallelOperationThreadCount = 10
            };
            await blob.UploadFromFileAsync(filePath, null, parallelOptions, null);
            stopwatch.Stop();
            Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
            Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
            Console.WriteLine();

            /// Parallel Upload
            /// 
            Console.WriteLine($"parallel upload:");
            blob = container.GetBlockBlobReference(sourceFile + ".parallel");
            Console.WriteLine($"\tto: {blob.Uri}");

            // var blockSize = 1024 * 1024 * 10; //10MB
            var blockSize = 1024 * 1024 * 5; //5MB
            // var blockSize = 1024 * 1024 * 1; //1MB
            var uploadedBlock = Enumerable.Empty<ListBlockItem>();

            if (await blob.ExistsAsync())
            {
                uploadedBlock = await blob.DownloadBlockListAsync();

                if (!uploadedBlock.Any())
                {
                    Console.WriteLine("\tcleanup");
                    await blob.DeleteIfExistsAsync();
                }
                else
                {
                    Console.WriteLine($"\tprevious upload {uploadedBlock.Count()} block(s) found!");
                    Console.Write($"\t\tpress Y for continue, N for delete and restart. [Y/n] ");

                    var readKey = true;
                    while (readKey)
                    {
                        var input = Console.ReadKey();
                        switch (input.Key)
                        {
                            case ConsoleKey.N:
                                Console.WriteLine("\n\tremove previous uploads");
                                readKey = false;
                                uploadedBlock = Enumerable.Empty<ListBlockItem>();
                                await blob.DeleteIfExistsAsync();
                                break;
                            case ConsoleKey.Enter:
                            case ConsoleKey.Y:
                                Console.WriteLine("\n\tcontinue to upload");
                                blockSize = (int)uploadedBlock.First().Length;
                                readKey = false;
                                break;
                        }
                    }
                }
            }

            var taskCount = (int)Math.Ceiling((double)fileSize / blockSize);
            var parallelize = 10;

            var taskParams = Enumerable
                                .Range(0, taskCount)
                                .Select(x =>
                                {
                                    var start = x * blockSize;
                                    var end = (int)Math.Min(start + blockSize - 1, fileSize);
                                    var length = end - start + 1;
                                    var blockId = Convert.ToBase64String(BitConverter.GetBytes(x));

                                    Func<Task<(byte[], string)>> get = async () =>
                                    {
                                        using (var sourceStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                                        using (var md5 = MD5.Create())
                                        {
                                            var buffer = new byte[length];
                                            sourceStream.Position = start;
                                            await sourceStream.ReadAsync(buffer, 0, length);
                                            var hash = Convert.ToBase64String(md5.ComputeHash(buffer));

                                            return (buffer, hash);
                                        }
                                    };
                                    return new { Sequence = x, N = x, BlockId = blockId, GetBufferAsync = get };
                                })
                                .Where(x => !uploadedBlock.Select(u => u.Name).Contains(x.BlockId))
                                .ToList();

            if (!taskParams.Any())
            {
                Console.WriteLine("\tno more upload task need to complete.");
                return;
            }

            Console.WriteLine($"\tblock size: {blockSize:##,#} bytes");
            Console.WriteLine($"\ttasks: {taskCount}");
            Console.WriteLine($"\tparallelize: {parallelize}");

            try
            {
                stopwatch.Restart();

                var blobRequest = new BlobRequestOptions()
                {

                };

                using (var concurrencySemaphore = new SemaphoreSlim(parallelize))
                {
                    var tasks = taskParams
                                    .Select(x => Task.Run(async () =>
                                    {
                                        await Task.Delay(x.N * 100);
                                        await concurrencySemaphore.WaitAsync();
                                        var (buffer, hash) = await x.GetBufferAsync();
                                        await blob
                                                .PutBlockAsync(x.BlockId, new MemoryStream(buffer), hash)
                                                .ContinueWith(x => concurrencySemaphore.Release());
                                        Console.WriteLine($"\t\t{x.N} {x.BlockId} : {hash}");
                                        return hash;
                                    }))
                                    .ToArray();

                    Task.WaitAll(tasks);
                }

                await blob.PutBlockListAsync(taskParams.Select(x => x.BlockId));
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
                Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
                Console.WriteLine();
            }
        }
    }
}
