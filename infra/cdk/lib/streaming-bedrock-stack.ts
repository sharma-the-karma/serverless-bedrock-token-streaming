import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as path from "node:path";

export class StreamingBedrockStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Define Lambda Function with Node.js 20.x
    const streamingFn = new lambda.Function(this, "BedrockStreamingFunction", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../../../lambda")),
      timeout: cdk.Duration.minutes(5), // Overcomes API Gateway 29s timeout!
      memorySize: 512,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        BEDROCK_MODEL_ID: "anthropic.claude-3-5-sonnet-20241022-v2:0",
      },
    });

    // 2. Grant permissions to Amazon Bedrock ConverseStream
    streamingFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "bedrock:InvokeModel",
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:ConverseStream",
        ],
        resources: ["*"],
      })
    );

    // 3. Add Function URL with RESPONSE_STREAM invoke mode
    const fnUrl = streamingFn.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      invokeMode: lambda.InvokeMode.RESPONSE_STREAM, // CRITICAL FOR STREAMING!
      cors: {
        allowedOrigins: ["*"],
        allowedMethods: [lambda.HttpMethod.ALL],
        allowedHeaders: ["*"],
      },
    });

    // 4. Wrap with CloudFront CDN using FunctionUrlOrigin
    const fnUrlDomain = cdk.Fn.select(2, cdk.Fn.split("/", fnUrl.url));

    const distribution = new cloudfront.Distribution(this, "StreamingCdn", {
      defaultBehavior: {
        origin: new origins.HttpOrigin(fnUrlDomain, {
          protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
        // Crucial: CachingDisabled prevents CloudFront from buffering SSE chunks
        cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
        originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
      },
    });

    // Outputs
    new cdk.CfnOutput(this, "FunctionUrl", {
      value: fnUrl.url,
      description: "Direct Lambda Function URL with Response Streaming",
    });

    new cdk.CfnOutput(this, "CloudFrontUrl", {
      value: `https://${distribution.distributionDomainName}`,
      description: "CloudFront Streaming Endpoint",
    });
  }
}
