using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using LibGit2Sharp;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace GhostVersionFunctionApp
{
    public static class UploadReleaseFunction
    {
        private static readonly Settings Settings = new Settings();
        private static readonly HttpClient HttpClient = new HttpClient();
        private static readonly JsonSerializerSettings SerializerSettings = new JsonSerializerSettings
        {
            MissingMemberHandling = MissingMemberHandling.Ignore,
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters =
            {
                new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AssumeUniversal }
            },
        };

        [Singleton]
        [FunctionName("ghost-release-processor")]
        public static async Task StartAsync(
            [TimerTrigger("0 0 16 * * *"
            #if DEBUG
            , RunOnStartup=true
            #endif
            )]TimerInfo myTimer,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Starting a new orchestrator with request data
            string instanceId = await starter.StartNewAsync("release-processor-orchestrator", null);

            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Started orchestration with ID = '{instanceId}'.");
        }

        [FunctionName("release-processor-orchestrator")]
        public static async Task RunAsync([OrchestrationTrigger] DurableOrchestrationContext context, ILogger log)
        {
            var processedRelease = context.CallActivityAsync<ReleaseInfo>("ghost-processed-release", "3.");
            var allReleases = context.CallActivityAsync<ReleaseInfo[]>("ghost-all-releases", "3.");
            var processed2Release = context.CallActivityAsync<ReleaseInfo>("ghost-processed-release", "2.");
            var all2Releases = context.CallActivityAsync<ReleaseInfo[]>("ghost-all-releases", "2.");

            await Task.WhenAll(processedRelease, allReleases, processed2Release, all2Releases);

            var releases = await context.CallActivityAsync<List<ReleaseInfo>>("ghost-remaining-releases", (processedRelease.Result, allReleases.Result));
            foreach (var release in releases)
            {
                await context.CallActivityAsync<string>("ghost-process-release", (Settings.GitRepoBranch, release));
            }

            var releases2 = await context.CallActivityAsync<List<ReleaseInfo>>("ghost-remaining-releases", (processed2Release.Result, all2Releases.Result));
            foreach (var release in releases2)
            {
                await context.CallActivityAsync<string>("ghost-process-release", (Settings.GitRepoBranchV2, release));
            }
        }

        [FunctionName("ghost-processed-release")]
        public static async Task<ReleaseInfo> GetProcessedReleaseAsync([ActivityTrigger]string releaseFilter, ILogger log)
        {
            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Loading latest processed Ghost release.");

            var message = new HttpRequestMessage(HttpMethod.Get, $"https://api.github.com/repos/{Settings.GitRepoOwner}/{Settings.GitRepoName}/releases?per_page=100");
            var byteArray = Encoding.ASCII.GetBytes($"{Settings.GitUserName}:{Settings.GitPassword}");
            message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            message.Headers.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue("Mozilla", "5.0"));
            var response = await HttpClient.SendAsync(message);

            var responseContent = await response.Content.ReadAsStringAsync();
            var releases = JsonConvert.DeserializeObject<List<Release>>(responseContent, SerializerSettings);

            var release = releases.FirstOrDefault(r => r.Name.StartsWith(releaseFilter, StringComparison.OrdinalIgnoreCase));

            if (release == null)
            {
                log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Latest processed Ghost {releaseFilter}x release: none.");

                return null;
            }
            else
            {
                log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Latest processed Ghost {releaseFilter}x release: {release.Name}.");

                return new ReleaseInfo()
                {
                    ReleaseName = release.Name,
                    ReleaseNotes = release.Body,
                    ReleaseUrl = release.Assets.FirstOrDefault()?.BrowserDownloadUrl.AbsoluteUri
                };
            }
        }

        [FunctionName("ghost-all-releases")]
        public static async Task<List<ReleaseInfo>> GetAllReleasesAsync([ActivityTrigger]string releaseFilter, ILogger log)
        {
            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Loading newest Ghost {releaseFilter}x releases.");

            var message = new HttpRequestMessage(HttpMethod.Get, $"https://api.github.com/repos/TryGhost/Ghost/releases?per_page=100");
            var byteArray = Encoding.ASCII.GetBytes($"{Settings.GitUserName}:{Settings.GitPassword}");
            message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            message.Headers.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue("Mozilla", "5.0"));
            var response = await HttpClient.SendAsync(message);

            var responseContent = await response.Content.ReadAsStringAsync();
            var releases = JsonConvert.DeserializeObject<List<Release>>(responseContent, SerializerSettings);

            var v2Releases = releases.Where(r => r.Name.StartsWith(releaseFilter, StringComparison.OrdinalIgnoreCase)).Select(r => new ReleaseInfo()
            {
                ReleaseName = r.Name,
                ReleaseNotes = r.Body,
                ReleaseUrl = r.Assets.FirstOrDefault()?.BrowserDownloadUrl.AbsoluteUri
            }).ToList();

            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Available Ghost {releaseFilter}x releases: {v2Releases.Count}.");

            return v2Releases;
        }

        [FunctionName("ghost-remaining-releases")]
        public static List<ReleaseInfo> DetermineRemainingReleasesAsync([ActivityTrigger]Tuple<ReleaseInfo, List<ReleaseInfo>> releaseInfo, ILogger log)
        {
            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Determining new Ghost releases that need processing.");

            var (processedRelease, allReleases) = releaseInfo;

            var remainingReleases = new List<ReleaseInfo>();

            if (processedRelease == null)
            {
                remainingReleases = allReleases;
                remainingReleases.Reverse();
            }
            else if (!string.IsNullOrEmpty(processedRelease.ReleaseName))
            {
                var i = allReleases.FindIndex(ar => ar.ReleaseName.Equals(processedRelease.ReleaseName, StringComparison.OrdinalIgnoreCase));

                if (i == -1)
                {
                    remainingReleases = allReleases;
                    remainingReleases.Reverse();
                }
                else if (i >= 0)
                {
                    remainingReleases = allReleases.GetRange(0, i);
                    remainingReleases.Reverse();
                }
            }
            else
            {
                log.Log(Microsoft.Extensions.Logging.LogLevel.Warning, "There was a problem determining the latest processed release.");
            }

            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"New Ghost releases that need processing: {remainingReleases.Count}.");

            return remainingReleases;
        }

        [FunctionName("ghost-process-release")]
        public static async Task ProcessReleaseAsync([ActivityTrigger]Tuple<string, ReleaseInfo> funcParams, ILogger log, ExecutionContext context)
        {
            var (branchName, releaseInfo) = funcParams;
            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Processing Ghost release: {releaseInfo.ReleaseName}.");
            log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Processing in branch: {branchName}.");

            var resourcesPath = context.FunctionAppDirectory;
            var repoPath = Path.GetFullPath(Path.Combine(resourcesPath, @"..\Target-" + DateTime.UtcNow.ToString("yyyyMMddTHHmmss")));
            try
            {
                var co = new CloneOptions
                {
                    CredentialsProvider = Settings.Handler,
                    BranchName = branchName
                };
                var gitPath = Repository.Clone($"https://github.com/{Settings.GitRepoOwner}/{Settings.GitRepoName}.git", repoPath, co);
                using (var repo = new Repository(gitPath))
                {
                    var repoDir = new DirectoryInfo(repoPath);
                    repoDir.Empty(true);

                    await repoDir.DownloadGhostVersion(releaseInfo.ReleaseUrl);

                    repoDir.EnrichPackageJson();

                    var azureResourcesDir = new DirectoryInfo(Path.Combine(resourcesPath, "AzureDeployment"));
                    azureResourcesDir.CopyFilesRecursively(repoDir);

                    Commands.Stage(repo, "*");

                    var author = new Signature(Settings.GitAuthorName, Settings.GitAuthorEmail, DateTime.Now);
                    var commit = repo.Commit($"Add v{releaseInfo.ReleaseName}", author, author);
                    var options = new PushOptions
                    {
                        CredentialsProvider = Settings.Handler
                    };
                    repo.Network.Push(repo.Branches[branchName], options);

                    await CreateRelease(releaseInfo.ReleaseName, releaseInfo.ReleaseNotes, branchName);
                }
            }
            catch (Exception e)
            {
                log.Log(Microsoft.Extensions.Logging.LogLevel.Error, e.Message);
                log.Log(Microsoft.Extensions.Logging.LogLevel.Error, e.StackTrace);
            }
            finally
            {
                log.Log(Microsoft.Extensions.Logging.LogLevel.Information, $"Finished processing Ghost release: {releaseInfo.ReleaseName}.");
            }
        }

        private static async Task CreateRelease(string releaseName, string releaseNotes, string branchName)
        {
            var data = new
            {
                tag_name = releaseName,
                target_commitish = branchName,
                name = releaseName,
                body = releaseNotes,
                draft = false,
                prerelease = false
            };
            var stringContent = new StringContent(JsonConvert.SerializeObject(data), Encoding.UTF8, "application/json");

            // You must set a user agent so that the CRLF requirement on the header parsing is met.
            // Otherwise you will get an excpetion message with "The server committed a protocol violation. Section=ResponseStatusLine"
            var message = new HttpRequestMessage(HttpMethod.Post, $"https://api.github.com/repos/{Settings.GitRepoOwner}/{Settings.GitRepoName}/releases")
            {
                Content = stringContent
            };
            var byteArray = Encoding.ASCII.GetBytes($"{Settings.GitUserName}:{Settings.GitPassword}");
            message.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray));
            message.Headers.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue("Mozilla", "5.0"));
            await HttpClient.SendAsync(message);
        }
    }
}