import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as path from "node:path";

export interface StreamingBedrockStackProps extends cdk.StackProps {
  corsOrigin?: string;
  originVerifySecret?: string;
}

export class StreamingBedrockStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: StreamingBedrockStackProps) {
    super(scope, id, props);

    const corsOrigin = props?.corsOrigin || "https://yourdomain.com";
    const originVerifySecret = props?.originVerifySecret || "ChangeMeInProductionSecretToken123!";

    // 1. Define Lambda Function with Node.js 22.x
    const streamingFn = new lambda.Function(this, "BedrockStreamingFunction", {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../../../lambda")),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        BEDROCK_MODEL_ID: "amazon.nova-pro-v1:0",
        ALLOWED_ORIGIN: corsOrigin,
        ORIGIN_VERIFY_SECRET: originVerifySecret,
      },
    });

    // 2. Scoped permissions to Amazon Bedrock ConverseStream & specific models
    streamingFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:ConverseStream",
        ],
        resources: [
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}::foundation-model/amazon.nova-pro-v1:0`,
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}::foundation-model/amazon.nova-lite-v1:0`,
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0`,
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}::foundation-model/anthropic.claude-3-7-sonnet-20250219-v1:0`,
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:inference-profile/*`,
          `arn:${cdk.Aws.PARTITION}:bedrock:*:*:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0`,
          `arn:${cdk.Aws.PARTITION}:bedrock:*:*:inference-profile/us.anthropic.claude-3-5-haiku-20241022-v1:0`,
        ],
      })
    );

    // 3. Add Function URL with RESPONSE_STREAM invoke mode
    const fnUrl = streamingFn.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      invokeMode: lambda.InvokeMode.RESPONSE_STREAM,
      cors: {
        allowedOrigins: [corsOrigin],
        allowedMethods: [lambda.HttpMethod.POST, lambda.HttpMethod.OPTIONS],
        allowedHeaders: ["Content-Type", "Authorization", "x-api-key", "x-origin-verify"],
      },
    });

    // 4. Wrap with CloudFront CDN using FunctionUrlOrigin with origin verification header
    const fnUrlDomain = cdk.Fn.select(2, cdk.Fn.split("/", fnUrl.url));

    const distribution = new cloudfront.Distribution(this, "StreamingCdn", {
      defaultBehavior: {
        origin: new origins.HttpOrigin(fnUrlDomain, {
          protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
          customHeaders: {
            "X-Origin-Verify": originVerifySecret,
          },
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
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
