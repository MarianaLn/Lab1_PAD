using System;
using System.ComponentModel;
using System.Text;
using System.Text.Json.Serialization;
using Common;
using Newtonsoft.Json;
using Sender;
namespace Subscriber
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Subscriber");

            string topic;

            Console.Write("Enter the topic: ");
            topic = Console.ReadLine().ToLower();

            var SubscriberSocket = new SubscriberSocket(topic);
            SubscriberSocket.Connect(SettingsBindableAttribute.BROKER_IP, SettingsBindableAttribute.BROKER_PORT);
            Console.WriteLine("Press any key to exit");
            Console.ReadLine();

        }
    }
}