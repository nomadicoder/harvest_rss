require 'rubygems'
require 'rss'
require 'rsolr'
require 'pry'
require 'securerandom'
require 'ruby-progressbar'

def harvest_rss (rss_source)
  open(rss_source) do |rss|
    batch_size = 100
    batch_thread = []
    feed = RSS::Parser.parse(rss_source)
    puts "Harvesting: #{feed.channel.title}"
    progressbar = ProgressBar.create(:title => "Item", :total => 1 + (feed.items.size / batch_size), format: "%t (%c/%C) %a |%B|")
    solr = RSolr.connect :url => 'http://localhost:8983/solr/blacklight-core'
    feed.items.each_slice(batch_size) do |batch|
      batch_thread << Thread.new {
        document_batch = []
        batch.each do | item |
          document_batch << ( add_rss_item ({item: item, copyright: feed.channel.copyright}) )
        end

        solr.add document_batch, add_attributes: { commitWithin: 10 }
        progressbar.increment
      }

    end

    solr.commit

    puts "Awaiting end"
    batch_thread.each { |t| t.join }
    puts "Done"
  end
end

def add_rss_item (args)
  item = args[:item]
  channel_str = get_channel(item.link)
  author, title = item.title.split(" - ")
  guid = Digest::MD5.hexdigest(item.guid.content ? item.guid.content : item.link)
  document = { id: guid,
               channel_facet: channel_str,
               author_t: author,
               author_display: author,
               title_t: title,
               title_display: title,
               pub_date: item.pubDate.year.to_s,
               release_date_display: item.pubDate.to_s,
               url_fulltext_display: item.link,
               text: item.description,
               description_display: item.description,
               guid_s: item.guid.content,
               copyright_s: args[:copyright],
               subject_topic_facet: item.itunes_keywords}
      
  document[:thumbnail_display] = item.enclosure.url if item.enclosure.type == "image/png"

  document
end

def get_channel (uri_str)
  uri = URI(uri_str)
  host = uri.host
  if host
    channel_str = host.split('.').first
  else
    channel_str = 'unknown'
  end
end

harvest_rss('completeArchiveWithImages.xml')
