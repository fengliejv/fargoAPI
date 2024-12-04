const express = require("express");
const {createProxyMiddleware} = require("http-proxy-middleware");
const zlib = require("zlib");

var https = require('https');
var http = require('http');
var fs = require('fs');
var path = "/home/lighthouse"

//同步读取密钥和签名证书
var options = {
    key: fs.readFileSync('/home/lighthouse/proxy/keys/proxy.xbanker.xyz.key'),
    cert: fs.readFileSync('/home/lighthouse/proxy/keys/proxy.xbanker.xyz_bundle.crt')
}
const app = express();
var httpsServer = https.createServer(options, app);
var httpServer = http.createServer(app);

const baseProxyTarget = "https://fargoinsight.glide.page";

// 定义需要替换的servicesName清单
const servicesList = [
    "firestore",
    "firebasestorage",
    "functions",
    "messaging",
    "auth",
    "database",
    "firestore-emulator",
    "identitytoolkit",
];

// 更新的domainReplacer函数，通过req获取当前的host
function domainReplacer(body, req) {
    // 获取请求头中的host
    const host = req.headers.host;
    regex = new RegExp(`\\b${"res"}\\.cloudinary\\.com\\b`, "g");
    body = body.replace(regex, `${host}/proxy/cloudinary/${"res"}`);

    // 对每一个serviceName进行替换操作
    servicesList.forEach((service) => {
        // 构建正则表达式，动态插入serviceName
        var regex = new RegExp(`\\b${service}\\.googleapis\\.com\\b`, "g");
        // 更新替换逻辑，插入当前请求的host
        body = body.replace(regex, `${host}/proxy/googleapis/${service}`);
    });
    return body;
}

// Response decompression function
function decompressResponse(proxyRes, callback) {
    const encoding = proxyRes.headers["content-encoding"];
    const bodyChunks = [];
    proxyRes.on("data", (chunk) => bodyChunks.push(chunk));
    proxyRes.on("end", () => {
        const bodyBuffer = Buffer.concat(bodyChunks);
        switch (encoding) {
            case "br":
                zlib.brotliDecompress(bodyBuffer, callback);
                break;
            case "gzip":
                zlib.gunzip(bodyBuffer, callback);
                break;
            case "deflate":
                zlib.inflate(bodyBuffer, callback);
                break;
            default:
                callback(null, bodyBuffer); // no compression
        }
    });
}

function createRouteProxy() {
    return createProxyMiddleware({
        router: function (req) {
            const serviceName = req.originalUrl.match(
                /\/proxy\/googleapis\/([^\/]+)(\/?.*)/,
            )[1];

            return `https://${serviceName}.googleapis.com`;
        },
        pathRewrite: function (path, req) {
            const serviceName = req.originalUrl.match(
                /\/proxy\/googleapis\/([^\/]+)(\/?.*)/,
            )[1];
            return path.replace(`/proxy/googleapis/${serviceName}`, "");
        },
        changeOrigin: true,
        logLevel: "debug",
        onProxyReq: (proxyReq, req, res) => {

            // 当请求体存在且是POST或PUT请求时
            if (req.body && (req.method === "POST" || req.method === "PUT")) {
                // 对请求体进行STRINGIFY，并设置正确的头部信息
                proxyReq.setHeader("Content-Type", "application/json");
                proxyReq.setHeader("Content-Length", Buffer.byteLength(bodyData));
                // 将请求体数据写入代理请求
                proxyReq.write(req.body);
                proxyReq.end(); // 确保结束请求
            }
        },
    });
}

function createCloudProxy() {
    return createProxyMiddleware({
        router: function (req) {
            return `https://res.cloudinary.com`;
        },
        pathRewrite: function (path, req) {
            const serviceName = req.originalUrl.match(
                /\/proxy\/cloudinary\/([^\/]+)(\/?.*)/,
            )[1];
            return path.replace(`/proxy/cloudinary/${serviceName}`, "");
        },
        changeOrigin: true,
        logLevel: "debug",
        onProxyReq: (proxyReq, req, res) => {
            // 当请求体存在且是POST或PUT请求时
            if (req.body && (req.method === "POST" || req.method === "PUT")) {
                // 对请求体进行STRINGIFY，并设置正确的头部信息
                proxyReq.setHeader("Content-Type", "image/webp");
                proxyReq.setHeader("Content-Length", Buffer.byteLength(bodyData));
                // 将请求体数据写入代理请求
                proxyReq.write(req.body);
                proxyReq.end(); // 确保结束请求
            }

        },
    });
}

function createSvgProxy() {
    return createProxyMiddleware({
        router: function (req) {
            return baseProxyTarget;
        },
        pathRewrite: function (path, req) {
            return path;
        },
        changeOrigin: true,
        logLevel: "debug",
        onProxyReq: (proxyReq, req, res) => {
        },
    });
}

// googleapis proxy
app.use("/proxy/googleapis/*", (req, res, next) => {
    const proxy = createRouteProxy();
    proxy(req, res, next);
});

//  proxy
app.use("/proxy/cloudinary/*", (req, res, next) => {
    const proxy = createCloudProxy();
    proxy(req, res, next);
});

app.use("/svg/stroke/*", (req, res, next) => {
    const proxy = createSvgProxy();
    proxy(req, res, next);
});


function cacheFile(body, req) {
    const fs = require('fs');
    var index = req.originalUrl.toString().indexOf("/static/js/");
    var filename = req.originalUrl.toString().substring(index + 10);
    if (req.originalUrl.lastIndexOf("/static/css/") > -1) {
        index = req.originalUrl.lastIndexOf("/static/css/");
        filename = req.originalUrl.toString().substring(index + 11);
    }
    filename = filename.replace(/\./g, "");
    let filePath = path + "/proxy/cache" + filename;
    try {
        fs.writeFileSync(filePath, body);
        console.log("文件已经被缓存到文件！");
    } catch (err) {
        console.error(`发生了错误：${err}`);
    }
}

app.use(
    "/static/js/*.",
    createProxyMiddleware({
        target: baseProxyTarget,
        changeOrigin: true,
        selfHandleResponse: true,
        onProxyRes: function (proxyRes, req, res) {
            console.log("/static/js/*.")
            decompressResponse(proxyRes, (err, decompressedBodyBuffer) => {
                // Handle any errors during decompression
                if (err) {
                    console.error("An error occurred during decompression:", err);
                    res.status(500).send("An error occurred.");
                    return;
                }

                // Convert decompressed body buffer to string
                let body = decompressedBodyBuffer.toString();

                // Apply the domain replacer to the body
                body = domainReplacer(body, req);

                // Set the header and send the modified body
                res.setHeader("Content-Type", "application/javascript");
                res.send(body);
                cacheFile(body, req)
            });
        },
    }),
);


// Specific JS files proxy configuration

// Proxy configuration for global proxy
app.use(
    "*",
    createProxyMiddleware({
        target: baseProxyTarget,
        changeOrigin: true,
        selfHandleResponse: true,
        onProxyRes: function (proxyRes, req, res) {
            console.log("*")

            decompressResponse(proxyRes, (err, decompressedBodyBuffer) => {
                let result;
                if (err) {
                    console.error("An error occurred:", err);
                    res.status(500).send("Error when decompressing the response body.");
                    return;
                }
                var filePath = path + "/proxy/cache/";
                var index = req.url.toString().indexOf("/static/js/");

                if (index > -1) {
                    var filename = req.url.substring(index + 11).replace(/\./g, "");
                    filePath = filePath + filename;
                } else {
                    index = req.url.toString().indexOf("/static/css/");
                    if (index > -1) {
                        var filename = req.url.substring(index + 12).replace(/\./g, "");
                        filePath = filePath + filename;
                    }
                }


                console.log("url:" + req.url + " filepath:" + filePath + " index:" + index)
                if (req.url.includes(".js")) {
                    res.setHeader("Content-Type", "application/javascript");
                }
                if (req.url.includes(".css")) {
                    res.setHeader("Content-Type", "text/css");
                }
                if (index > -1) {
                    let exists = fs.existsSync(filePath)
                    if (exists) {
                        let data = fs.readFileSync(filePath)
                        res.setHeader("Content-Length", Buffer.byteLength(data));
                        result = data.toString();
                        console.log("使用缓存文件")
                    }
                    if (!exists) {
                        console.log('缓存文件不存在');
                        let body = decompressedBodyBuffer.toString();
                        let rewrittenBody = domainReplacer(body, req);
                        if (req.url.includes(".css")) {
                            rewrittenBody = `/*rewrited*/\n${rewrittenBody}`;
                        }
                        if (req.url.includes("/static/")) {
                            cacheFile(rewrittenBody, req)
                        }
                        res.setHeader("Content-Length", Buffer.byteLength(Buffer.from(rewrittenBody)));
                        result = rewrittenBody;
                    }
                    // fs.exists(filePath, function (exists) {
                    //     if (exists) {
                    //         fs.readFile(filePath, (err, data) => {
                    //             if (err) {
                    //                 console.error(`无法读取文件 ${filePath}:`, err);
                    //             } else {
                    //                 console.log("文件读取成功")
                    //
                    //             }
                    //         });
                    //     }
                    //     if (!exists) {
                    //         console.log('缓存文件不存在');
                    //         let body = decompressedBodyBuffer.toString();
                    //         let rewrittenBody = domainReplacer(body, req);
                    //         if (req.url.includes(".css")) {
                    //             rewrittenBody = `/*rewrited*/\n${rewrittenBody}`;
                    //         }
                    //         if (req.url.includes("/static/")) {
                    //             cacheFile(rewrittenBody, req)
                    //         }
                    //         result = rewrittenBody;
                    //     }
                    // });
                    if (result !== null) {
                        // Calling brotliCompress method
                        // zlib.brotliCompress(result, (err, buffer) => {
                        //
                        //     //console.log(buffer.toString('base64'));
                        //     res.send(buffer);
                        // });

                        // let encoding = proxyRes.headers["content-encoding"];
                        // console.log(encoding)
                        // if (encoding === "br") {
                        // 	console.log("fuck")
                        //     res.setHeader("Content-Encoding", 'br');
                        //     res.setHeader("Access-Control-Allow-Origin", '*');
                        //     res.setHeader("Vary", 'Accept-Encoding');
                        //     res.send(zlib.brotliCompressSync(Buffer.from(result)))
                        //     //res.send(zlib.brotliCompressSync(decompressedBodyBuffer))
                        //     //res.send(zlib.brotliCompressSync(Buffer.from("fuck")))
                        // } else {
                        //     res.send(result)
                        // }
                        res.writeHead(200, {
                            "Content-Type": "text/html;charset=utf-8",
                            "Content-Encoding": "gzip", // 告诉浏览器我们是通过gzip压缩的
                        });

                        const readStream = fs.createReadStream(filePath);
                        const gzip = zlib.createGzip();
                        // res本质就是一个可写流
                        // 在数据返回之前使用gzip压缩数据
                        readStream.pipe(gzip).pipe(res);

                    }
                } else {
                    let body = decompressedBodyBuffer.toString();
                    let rewrittenBody = domainReplacer(body, req);
                    if (req.url.includes(".css")) {
                        rewrittenBody = `/*rewrited*/\n${rewrittenBody}`;
                    }
                    res.setHeader("Content-Length", Buffer.byteLength(Buffer.from(rewrittenBody)));
                    res.send(rewrittenBody);
                }

            });
        },
    }),
);

const PORT = process.env.PORT || 3000;
httpsServer.listen(PORT, () => {
    console.log(`服务正在端口 ${PORT} 上运行`);
});
