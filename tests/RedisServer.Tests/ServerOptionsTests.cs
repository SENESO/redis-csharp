using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using RedisServer.Options;
using RedisServer.Transport;

namespace RedisServer.Tests;

/// <summary>
/// Configuration plumbing for AUTH and TLS: flag parsing, and loading the
/// PKCS#12 server certificate (the TLS handshake itself is verified live
/// by a Python ssl client — see the README's compatibility section).
/// </summary>
public sealed class ServerOptionsTests
{
    [Fact]
    public void Defaults_HaveNoPasswordAndNoTls()
    {
        var options = ServerOptions.FromArgs(Array.Empty<string>());
        Assert.Null(options.RequirePass);
        Assert.Equal(0, options.TlsPort);
        Assert.Null(options.TlsCertFile);
    }

    [Fact]
    public void RequirePass_AndTlsFlags_AreParsed()
    {
        var options = ServerOptions.FromArgs(new[]
        {
            "-p", "6380", "--requirepass", "s3cret",
            "--tls-port", "7379", "--tls-cert-file", "/tmp/cert.pfx", "--tls-cert-password", "pfxpw"
        });
        Assert.Equal(6380, options.Port);
        Assert.Equal("s3cret", options.RequirePass);
        Assert.Equal(7379, options.TlsPort);
        Assert.Equal("/tmp/cert.pfx", options.TlsCertFile);
        Assert.Equal("pfxpw", options.TlsCertPassword);
    }

    [Fact]
    public void TlsWithoutACertificateFile_IsRejectedAtLoad()
    {
        var options = new ServerOptions { TlsPort = 7379 };
        var ex = Assert.Throws<InvalidOperationException>(() => TcpServer.LoadServerCertificate(options));
        Assert.Contains("--tls-cert-file", ex.Message);
    }

    [Fact]
    public void GeneratedPfx_LoadsWithItsPrivateKey()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        var path = Path.Combine(Path.GetTempPath(), $"redis-csharp-tls-{Guid.NewGuid():N}.pfx");
        try
        {
            File.WriteAllBytes(path, cert.Export(X509ContentType.Pfx, "pfxpw"));
            var options = new ServerOptions { TlsPort = 7379, TlsCertFile = path, TlsCertPassword = "pfxpw" };
            using var loaded = TcpServer.LoadServerCertificate(options);
            Assert.True(loaded.HasPrivateKey);
            Assert.Equal(cert.Thumbprint, loaded.Thumbprint);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
