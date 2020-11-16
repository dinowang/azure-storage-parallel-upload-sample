using System;
using System.Collections.Generic;
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
using Newtonsoft.Json;
using Polly;

namespace Upload
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                                .SetBasePath(Directory.GetCurrentDirectory())
                                .AddJsonFile("appsettings.json", false)
                                .AddJsonFile("appsettings.Debug.json", false)
                                .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("NETCORE_ENVIRONMENT")}.json", optional: true)
                                .Build();

            var storageConnectionString = config.GetValue<string>("StorageConnectionString");
            var sourceUri = new Uri(config.GetValue<string>("SourceUri"));
            var sourceFile = config.GetValue<string>("SourceFile");

            if (string.IsNullOrEmpty(sourceFile))
            {
                sourceFile = Path.GetFileName(sourceUri.LocalPath);
            }

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

            // var fileInfo2 = new FileInfo(sourceFile);
            // Console.WriteLine($"origin: {fileInfo.Name} => {fileInfo2.Length}");
            // var b1 = container.GetBlockBlobReference("Build-2019-Keynote-Satya.mp4.parallel");
            // b1.FetchAttributes();
            // Console.WriteLine($"    b1: {b1.Name} => {b1.Properties.Length}");
            // var b2 = container.GetBlockBlobReference("Build-2019-Keynote-Satya.mp4.azcopy");
            // b2.FetchAttributes();
            // Console.WriteLine($"    b2: {b2.Name} => {b2.Properties.Length}");
            // return;

            var stopwatch = new Stopwatch();

            /// Blob
            /// 
            Console.WriteLine($"\tsize: {fileSize:##,#} bytes");
            Console.WriteLine();

            /// Simple Upload
            /// 
            // Console.WriteLine($"simple upload:");
            // var blob = container.GetBlockBlobReference(Path.GetFileName(sourceFile) + ".simple");
            // Console.WriteLine($"\tto: {blob.Uri}");
            // if (await blob.ExistsAsync())
            // {
            //     Console.WriteLine($"\tcleanup");
            //     await blob.DeleteIfExistsAsync();
            // }
            // stopwatch.Start();
            // await blob.UploadFromFileAsync(filePath);
            // stopwatch.Stop();
            // Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
            // Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
            // Console.WriteLine();

            /// Simple Upload with BlobRequestOptions
            /// 
            // Console.WriteLine($"simple upload with `ParallelOperationThreadCount`:");
            // blob = container.GetBlockBlobReference(Path.GetFileName(sourceFile) + ".simple-parallel");
            // Console.WriteLine($"\tto: {blob.Uri}");
            // if (await blob.ExistsAsync())
            // {
            //     Console.WriteLine($"\tcleanup");
            //     await blob.DeleteIfExistsAsync();
            // }
            // stopwatch.Restart();
            // var parallelOptions = new BlobRequestOptions
            // {
            //     ParallelOperationThreadCount = 10
            // };
            // await blob.UploadFromFileAsync(filePath, null, parallelOptions, null);
            // stopwatch.Stop();
            // Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
            // Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
            // Console.WriteLine();

            /// Parallel Upload
            /// 
            Console.WriteLine($"parallel upload:");
            var blob = container.GetBlockBlobReference(Path.GetFileName(sourceFile) + ".parallel");
            Console.WriteLine($"\tto: {blob.Uri}");

            // var blockSize = 1024 * 1024 * 10; //10MB
            // var blockSize = 1024 * 1024 * 5; //5MB
            var blockSize = 1024 * 1024 * 100; //100MB
            // var blockSize = 1024 * 1024 * 1; //1MB

            var uploadedBlock = Enumerable.Empty<ListBlockItem>();

            try
            {
                uploadedBlock = await blob.DownloadBlockListAsync(BlockListingFilter.Uncommitted,
                                                                  AccessCondition.GenerateEmptyCondition(),
                                                                  new BlobRequestOptions { },
                                                                  new OperationContext { });
            }
            catch (StorageException)
            {
            }

            if (!uploadedBlock.Any())
            {
                // Console.WriteLine("\tcleanup");
                // await blob.DeleteIfExistsAsync();
            }
            else
            {
                Console.WriteLine($"\tprevious upload {uploadedBlock.Count()} block(s) found!");
                uploadedBlock.ToList().ForEach(x =>
                {
                    var n = BitConverter.ToInt16(Convert.FromBase64String(x.Name));
                    Console.WriteLine($"\t\tblock: {x.Name} ({n}), length: {x.Length}, committed: {x.Committed}");
                });
                Console.Write($"\t\tpress Y for continue, N for new upload, C for commit to file. [Y/n/c] ");

                var readKey = true;
                while (readKey)
                {
                    var input = Console.ReadKey();
                    switch (input.Key)
                    {
                        case ConsoleKey.C:
                            await blob.PutBlockListAsync(uploadedBlock.Select(x => x.Name));
                            return;
                        case ConsoleKey.N:
                            readKey = false;
                            uploadedBlock = Enumerable.Empty<ListBlockItem>();
                            // await blob.DeleteIfExistsAsync();
                            break;
                        case ConsoleKey.Enter:
                        case ConsoleKey.Y:
                            Console.WriteLine("\n\tcontinue to upload");
                            blockSize = (int)uploadedBlock.First().Length;
                            readKey = false;
                            break;
                    }
                }
                Console.WriteLine();
            }


            var taskCount = (int)Math.Ceiling((double)fileSize / blockSize);
            var parallelize = 5;

            var taskParams = Enumerable
                                .Range(1, taskCount)
                                .Select(x =>
                                {
                                    long start = (x - 1) * (long)blockSize;
                                    long end = Math.Min(start + blockSize - 1, fileSize - 1);
                                    int length = (int)(end - start + 1);
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

            try
            {
                if (!taskParams.Any())
                {
                    // there is no more blocks needed to upload

                    uploadedBlock = await blob.DownloadBlockListAsync(BlockListingFilter.Uncommitted,
                                                                    AccessCondition.GenerateEmptyCondition(),
                                                                    new BlobRequestOptions { },
                                                                    new OperationContext { });

                    await blob.PutBlockListAsync(uploadedBlock.Select(x => x.Name));
                }
                else
                {
                    // upload blocks

                    Console.WriteLine($"\tblock size: {blockSize:##,#} bytes");
                    Console.WriteLine($"\ttasks: {taskCount}");
                    Console.WriteLine($"\tparallelize: {parallelize}");

                    stopwatch.Restart();

                    var completedCount = 0L;

                    using (var concurrencySemaphore = new SemaphoreSlim(parallelize))
                    {
                        var policy = Policy
                                        .Handle<Exception>()
                                        .RetryForeverAsync(x => Console.WriteLine($"\t\tthread {Thread.CurrentThread.ManagedThreadId,5:#####} retry after {x.GetType().Name}: {x.Message}"));

                        var tasks = new List<Task>();

                        foreach (var taskParam in taskParams)
                        {
                            await concurrencySemaphore.WaitAsync();

                            var (buffer, hash) = await taskParam.GetBufferAsync();
                            var innerStopwatch = new Stopwatch();
                            innerStopwatch.Start();


                            tasks.Add(policy.ExecuteAsync(() => blob.PutBlockAsync(taskParam.BlockId, new MemoryStream(buffer), hash)
                                                                    .ContinueWith(x =>
                                                                    {
                                                                        innerStopwatch.Stop();
                                                                        Interlocked.Increment(ref completedCount);
                                                                        concurrencySemaphore.Release();
                                                                        var progress = (completedCount / (double)taskCount) * 100;
                                                                        Console.WriteLine($"\t\tthread {Thread.CurrentThread.ManagedThreadId,5:#####}, block {taskParam.N,3:###}, {progress,5:###.#}% completed, length {buffer.Length}, elapsed {innerStopwatch.Elapsed}");
                                                                    })));
                        }
                        await Task.WhenAll(tasks);
                    }
                    // await blob.PutBlockListAsync(taskParams.Select(x => x.BlockId));

                    uploadedBlock = await blob.DownloadBlockListAsync(BlockListingFilter.Uncommitted,
                                                                    AccessCondition.GenerateEmptyCondition(),
                                                                    new BlobRequestOptions { },
                                                                    new OperationContext { });

                    await blob.PutBlockListAsync(uploadedBlock.Select(x => x.Name));
                }

                await blob.FetchAttributesAsync();
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine($"\telapsed: {stopwatch.ElapsedMilliseconds:##,#} ms");
                Console.WriteLine($"\tthroughput: {(fileSize / stopwatch.ElapsedMilliseconds):##,#} bytes/sec (~{(fileSize / stopwatch.ElapsedMilliseconds) * 8:##,#} bps)");
                if (blob != null && blob.Properties != null)
                {
                    Console.WriteLine($"\tblob: {blob.Properties.Length:##,#} bytes, {blob.Properties.Created}");
                }
                Console.WriteLine();
            }
        }
    }
}
